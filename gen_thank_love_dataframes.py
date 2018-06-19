from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pymysql.err import InternalError, OperationalError
import sys, os
import pandas as pd
import numpy as np

from datetime import datetime as dt
from datetime import timedelta as td

import mwclient
import mwviews

from multiprocessing import Pool
    
from time import sleep
import json
import click





@click.command()
@click.option('--conf', default='test',
              help='the json file to look for in configs without `.json`')
def main():
    thank_love = 'love'

    datadir = os.path.join('data', langcode)

    db_prefix = '{}wiki_p'.format(langcode)

    site = mwclient.Site(('https', f'{langcode}.wikipedia.org'), path = '/w/')





    os.makedirs(datadir, exist_ok=True)

    constr = 'mysql+pymysql://{user}:{pwd}@{host}/DB?charset=utf8'.format(user=os.environ['MYSQL_USERNAME'],
                                                          pwd=os.environ['MYSQL_PASSWORD'],
                                                          host=os.environ['MYSQL_HOST'])
    con = create_engine(constr, encoding='utf-8')

    con.execute(f'use {db_prefix}')

    def wmftimestamp(bytestring):
        s = bytestring.decode('utf-8')
        return dt.strptime(s, '%Y%m%d%H%M%S')

    def decode_or_nouser(b):
        return b.decode('utf-8') if b else '#nosuchuser'


    # In[46]:


    #all_thanks_sql = f"select log_timestamp, log_title, log_user_text from logging where log_type = 'thanks'"
    thanks_sql = f"""select timestamp,
            receiver, 
            ru.user_id as receiver_id,
            sender,
            su.user_id as sender_id
    from
    (select log_timestamp as timestamp, replace(log_title, '_', ' ') as receiver, log_user_text as sender from {db_prefix}.logging where log_type = 'thanks') t
    left join {db_prefix}.user ru on ru.user_name = t.receiver
    left join {db_prefix}.user su on su.user_name = t.sender
    """

    love_sql = f"""select wll_timestamp as timestamp, 
    wll_receiver as receiver, 
    wll_receiver as receiver_id, 
    wll_sender as sender, 
    wll_sender as sender_id,
    wll_type
    from eswiki_p.wikilove_log"""

    thank_df = pd.read_sql(thanks_sql, con)

    #thank_df = thank_df.rename(mapper=mapper, axis='columns')

    thank_df['receiver'] = thank_df['receiver'].apply(decode_or_nouser)
    thank_df['sender'] = thank_df['sender'].apply(decode_or_nouser)
    thank_df['timestamp'] = thank_df['timestamp'].apply(wmftimestamp)


    # In[48]:


    love_df = pd.read_sql(love_sql, con)


    # ### note on thank sizes
    # #### june 2018
    # + enwiki has 1.5m thanks ~130megs
    # + plwiki has 81k thanks ~2.5meg

    # ### user id for each user
    # find na names. try and resolve them
    # 
    # create a many-to-one map where key is name, value is id
    # 
    # then do an apply

    # In[6]:


    receiver_noid = thank_df[pd.isnull(thank_df['receiver_id'])]['receiver'].unique()
    sender_noid = thank_df[pd.isnull(thank_df['sender_id'])]['sender'].unique()
    user_noid = set()
    user_noid.update(receiver_noid)
    user_noid.update(sender_noid)
    user_noid.discard('#nosuchuser') # this is the value that gets inserted when someone has a completely delete profile
    print(f'there were {len(user_noid)} profiles which did not have id and might be moved users')

    def try_follow_user_redirect(user):
    #     print(user)
        page = site.Pages['User:{user}'.format(user=user)]
        text=page.text()
        "user: {user}\n pagetext: {pagetext}".format(user=user, pagetext=text)
        try:
            redir_name = text.split(":")[1].split(']]')[0]
            return redir_name
        except IndexError:
            return None

    actual_moves = {user: try_follow_user_redirect(user) for user in user_noid if try_follow_user_redirect(user)}


    def get_id(user):
        rec_user_df = thank_df[thank_df['receiver']==user]
        sen_user_df = thank_df[thank_df['sender']==user]
        if len(rec_user_df) > 0:
            user_id = rec_user_df['receiver_id'].values[0]
            return user_id
        else:
            user_id = sen_user_df['sender_id'].values[0]
            return user_id

    for direction in ['sender','receiver']:
        for oldname, newname in actual_moves.items():
            user_id = get_id(newname)
            print(f'going to replace {oldname} with {user_id}')
            thank_df.loc[thank_df[direction] == oldname, f'{direction}_id'] = user_id


    ## cast everything back to ints
    thank_df['receiver_id'] = thank_df['receiver_id'].fillna(-1).astype(int)
    thank_df['sender_id'] = thank_df['sender_id'].fillna(-1).astype(int)


    # In[8]:


    len(user_ids)


    # In[9]:


    get_ipython().system('ls data/pl/user_histories/ | wc -l')


    # In[11]:


    def make_rev_sql(user_id):
        sql= f'''
        select rev_timestamp from {db_prefix}.revision r
        where rev_user = {user_id}'''
    #     print(sql)
        return sql

    def get_and_save_rev_history(strvar, sql_fn, ddir, pickle_filename):
        sql = sql_fn(strvar)
        #print(rev_user_sql)
        MAXRETRIES = 5
        retries = 0
        while (retries < MAXRETRIES):
            sleep(retries**2)

            try:
                df = pd.read_sql(sql, con)
                df['rev_timestamp'] = df['rev_timestamp'].apply(wmftimestamp)
                print('{} df has length {}'.format(strvar, len(df)))
                pickle_path = os.path.join(ddir, pickle_filename)
                df.to_pickle(pickle_path)
            except ProgrammingError:
                print(f"Couldnt execute: {sql}")
                retries += 1
            except InternalError:
                print(f"internal error: {sql}")
                retries += 1
            except OperationalError:
                print('probably lost connection')
                retries += 1

    user_ids = set()
    user_ids.update(thank_df['receiver_id'].values)
    user_ids.update(thank_df['sender_id'].values)

    print(f'we found {len(user_ids)} user ids for which to get history')
    userhistdir = os.path.join(datadir,'user_histories')
    os.makedirs(userhistdir, exist_ok=True)
    print(f'using user history directory {userhistdir}')

    def proc_user(user_id):
    #     print('doing {}'.format(user_id))
        # check that a user id was able to be found
        if user_id >= 0:
        #     print('working on {}'.format(user))
            pickle_filename = '{}.pickle'.format(user_id)
            existing_files = os.listdir(os.path.join(datadir, 'user_histories'))
    #         print(f'there are already {len(existing_files)} existing files')
            if not pickle_filename in existing_files:
                get_and_save_rev_history(user_id, make_rev_sql, userhistdir, pickle_filename)
            else:
                #print('we already did: {}'.format(user))
                pass
        else:
            #we couldn't find an id
            pass

    with Pool(10) as p:
        res = p.map_async(proc_user, user_ids)
        res.get()

    print('all done getting user history')


    # In[7]:


    # # for dir_type in ['user_histories','page_histories']:
    # data_dir = 'data/user_histories/'
    # ls = os.listdir(data_dir)
    # pickles = [f for f in ls if f.endswith('.pickle')]
    # for pickle in pickles:
    #     fullpath = os.path.join(data_dir, pickle)
    #     fastfile = fullpath+".fast"
    #     if not os.path.isfile(fastfile):
    #         df = pd.read_pickle(fullpath)
    #         df['rev_timestamp'] = df['rev_timestamp'].apply(wmftimestamp)
    #         df.to_pickle(fastfile)


    # In[8]:


    # data_dir = 'data/page_histories/'
    # ls = os.listdir(data_dir)
    # oldpicks = [f for f in ls if f.endswith('.pickle')]
    # for pickle in oldpicks:
    #     fullpath = os.path.join(data_dir, pickle)
    #     fastfile = fullpath+".fast"
    #     if not os.path.isfile(fastfile):
    #         df = pd.read_pickle(os.path.join(data_dir, pickle))
    #         df['rev_timestamp'] = df['rev_timestamp'].apply(wmftimestamp)
    #         df['user_name'] = df['user_name'].apply(decode_or_nouser)
    #         df.to_pickle(os.path.join(data_dir, '{}.fast'.format(pickle)))


    # ## make revision features from pickles 
    # ### number of previous edits
    # + note users who are no longer in the database (maybe deleted or changed names) return value nan

    # In[9]:


    ## Checkpoint
    # thank_df = pd.read_pickle('data/misc/thank_pl_2.pickle')


    # In[10]:


    # user_name_id_df = pd.read_pickle('data/misc/username_id_df_pl_2.pickle')


    # In[15]:


    missing_users = set()

    def load_userid_df(userid):
        '''returns none if userid is not found'''
        try:
            pickle_loc = os.path.join(datadir, 'user_histories', '{}.pickle'.format(userid))
            df = pd.read_pickle(pickle_loc)
            return df, True
        except FileNotFoundError:
            missing_users.add(userid)
    #             usercache[userid] = float(nan) # don't cache to save on headaches
            return pd.DataFrame(), False

    usercache = {}
    def num_prev_edits(userid, prior_to):
        return num_edits_during(userid, prior_to, future=None)

    def num_edits_during(userid, timestamp, future=None):
        '''future in days, if none all past'''
        if not userid in usercache.keys():
            df, user_exists = load_userid_df(userid)
            if not user_exists:
                return 
            else:
                usercache[userid] = df
        else:
            df = usercache[userid]


        if not future:
            time_cond = df['rev_timestamp'] < timestamp
            return len(df[time_cond])
        else:
            high_end = timestamp + td(days=future)
            tc1 = df['rev_timestamp'] > timestamp
            tc2 = df['rev_timestamp'] <= high_end
            return len(df[(tc1) & (tc2)])

    firsteditcache = {}
    def first_edit(userid):
        if not userid in firsteditcache.keys():
            df, user_exists = load_userid_df(userid)
            if not user_exists:
                return float('nan')
            else:
                mindate = df['rev_timestamp'].min()
                firsteditcache[userid] = mindate
                return mindate
        else:
            return firsteditcache[userid]

    pagecache = {}
    def user_edits_on_page_during(user, page, timestamp, before_or_after):
        '''number of times users edited page either before or n months after'''
        if not page in pagecache.keys():
            data_dir = 'data/page_histories'
            pickle = os.path.join(data_dir, '{}.pickle'.format(page))
            df = pd.read_pickle(pickle)
            pagecache[page] = df
        else:
            df = pagecache[page]

        user_cond = (df['user_name'] == user)

        if before_or_after == 'before':
            time_cond = df['rev_timestamp'] < timestamp
            return len(df[(time_cond)  & (user_cond)])
        elif isinstance(before_or_after, int):
            high_end = timestamp + td(days=before_or_after)
            tc1 = df['rev_timestamp'] > timestamp
            tc2 = df['rev_timestamp'] <= high_end
            return len(df[(tc1) & (tc2)  & (user_cond)])


    thankcache = {}
    def thank_another(user, role, timestamp,  future):
        cachekey = f'{user}_{role}'
        if not cachekey in thankcache.keys():
            user_cond = (thank_df[role] == user)
            df = thank_df[user_cond]
            thankcache[user] = df
        else:
            df = thankcache[cachekey]

        if not future:
            time_cond = df['timestamp'] < timestamp
            return len(df[time_cond])
        else:
            high_end = timestamp + td(days=future)
            tc1 = df['timestamp'] > timestamp
            tc2 = df['timestamp'] <= high_end
            return len(df[(tc1) & (tc2)])


    # In[43]:


    do_quick = True
    if do_quick:
        thank_df_full = thank_df
        thank_df = thank_df_full[:1000]


    # In[16]:


    thank_df['receiver_prev_received'] = thank_df.apply(lambda row: thank_another(user=row[1], role='receiver', timestamp=row[0], future=None), axis=1)

    thank_df['receiver_prev_sent'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=None), axis=1)

    thank_df['sender_prev_received'] = thank_df.apply(lambda row: thank_another(user=row[3], role='receiver', timestamp=row[0], future=None), axis=1)

    thank_df['sender_prev_sent'] = thank_df.apply(lambda row: thank_another(user=row[3], role='sender', timestamp=row[0], future=None), axis=1)


    # In[17]:


    conticols = ["receiver_prev_received","sender_prev_received","sender_prev_sent","receiver_prev_sent"]
    for col in conticols:
        indcol = "{col}_indicator".format(col=col)
        thank_df[indcol] = thank_df[col].apply(lambda x: x>0)


    # In[19]:


    len(thank_df)


    # In[20]:


    thank_df['receiver_prev_edits'] = thank_df.apply(lambda row: num_prev_edits(userid=row[2], prior_to=row[0]), axis=1)


    # In[21]:


    thank_df['sender_prev_edits'] = thank_df.apply(lambda row: num_prev_edits(userid=row[4], prior_to=row[0]), axis=1)


    # In[22]:


    thank_df['sender_first_edit'] = thank_df['sender_id'].apply(first_edit)
    thank_df['receiver_first_edit'] = thank_df['receiver_id'].apply(first_edit)


    # In[23]:


    thank_df['sender_edits_1d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=1), axis=1)


    # In[ ]:


    thank_df['sender_edits_30d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=30), axis=1)

    thank_df['sender_edits_90d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=90), axis=1)
    thank_df['sender_edits_180d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=180), axis=1)

    thank_df['receiver_edits_1d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=1), axis=1)
    thank_df['receiver_edits_30d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=30), axis=1)
    thank_df['receiver_edits_90d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=90), axis=1)
    thank_df['receiver_edits_180d_after'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=180), axis=1)


    # In[25]:


    thank_df['receiver_thank_another_1d_after'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=1), axis=1)

    thank_df['receiver_thank_another_30d_after'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=30), axis=1)

    thank_df['receiver_thank_another_90d_after'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=90), axis=1)

    thank_df['receiver_thank_another_180d_after'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=180), axis=1)


    # ## tests

    # In[30]:


    def natural_integer_test(seq):
        '''tests if a series is a natural integer sequence using a property'''
        l = len(seq) - 1 #minus 1 because we always start with a zero
        sum_identity = (l*(l+1))/2
        assert sum_identity == seq.sum()

    def monotonic(x):
        if np.any(np.isnan(x)):
            return True
        else:
            assert np.all(np.diff(x) >= 0)   

    most_sender = thank_df['sender'].value_counts().index[0]

    most_sender_r = thank_df[thank_df['receiver']=='most_sender']
    most_sender_s = thank_df[thank_df['sender']=='most_sender']
    most_sender_rpr = most_sender_r['receiver_prev_received']
    most_sender_rps = most_sender_r['receiver_prev_sent']
    most_sender_sps = most_sender_s['sender_prev_sent']
    most_sender_spr = most_sender_s['sender_prev_received']

    natural_integer_test(most_sender_rpr)
    natural_integer_test(most_sender_sps)
    monotonic(most_sender_spr)
    monotonic(most_sender_rps)


    # ## Testing previous edits

    # In[31]:


    for usercol, featcol in (('sender', 'sender_prev_edits'), ('receiver','receiver_prev_edits')):
        print(usercol, featcol)
        for name, group in thank_df.groupby(usercol):
            monotonic(group[featcol])
    print('all clear')


    # In[32]:


    for name, group in thank_df.groupby('sender'):
        assert len(group['sender_first_edit'].unique()) == 1


    # ## previous and future edits on page

    # In[ ]:


    # thank_df['sender_prev_edits_on_page'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[2], page=row[3], timestamp=row[0], before_or_after='before'), axis=1)


    # In[ ]:


    # thank_df['receiver_prev_edits_on_page'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[1], page=row[3], timestamp=row[0], before_or_after='before'), axis=1)


    # In[ ]:


    # thank_df['sender_edits_on_page_1d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[2], page=row[3], timestamp=row[0], before_or_after=1), axis=1)
    # thank_df['sender_edits_on_page_30d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[2], page=row[3], timestamp=row[0], before_or_after=30), axis=1)
    # thank_df['sender_edits_on_page_90d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[2], page=row[3], timestamp=row[0], before_or_after=90), axis=1)
    # thank_df['sender_edits_on_page_180d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[2], page=row[3], timestamp=row[0], before_or_after=180), axis=1)


    # In[ ]:


    #thank_df['sender_edits_on_page_180d_after'].value_counts()


    # In[ ]:


    # thank_df['receiver_edits_on_page_1d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[1], page=row[3], timestamp=row[0], before_or_after=1), axis=1)
    # thank_df['receiver_edits_on_page_30d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[1], page=row[3], timestamp=row[0], before_or_after=30), axis=1)
    # thank_df['receiver_edits_on_page_90d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[1], page=row[3], timestamp=row[0], before_or_after=90), axis=1)
    # thank_df['receiver_edits_on_page_180d_after'] = thank_df.apply(lambda row: user_edits_on_page_during(user=row[1], page=row[3], timestamp=row[0], before_or_after=180), axis=1)


    # ## save the data and back it up

    # In[ ]:


    # rearrange = ['timestamp', 'receiver', 'sender', 'log_page', 
    #              'receiver_prev_received', 'receiver_prev_received_indicator',
    #              'receiver_prev_sent', 'receiver_prev_sent_indicator',
    #              'sender_prev_received', 'sender_prev_received_indicator',
    #              'sender_prev_sent', 'sender_prev_sent_indicator',
    # #             'receiver_prev_edits', 'sender_prev_edits']
    # thank_df[rearrange].to_pickle('data/misc/thank_pl_3.pickle')


    # In[34]:


    outputdir = os.path.join(datadir, 'outputs')


    # In[35]:


    os.makedirs(outputdir, exist_ok=True)


    # In[40]:


    todaystr = dt.today().strftime('%Y%m%d')


    # In[42]:


    outfile = outfile = os.path.join(outputdir, f'wikithank_{langcode}_{todaystr}.csv')
    thank_df.to_csv(outfile, index=False)

if __name__ == '__main__':
    main()