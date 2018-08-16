from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import sys, os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np

from datetime import datetime as dt
from datetime import timedelta as td

import mwclient
import multiprocessing
from multiprocessing import Pool, cpu_count

import time
from time import sleep
import random
import json
import click

import warnings
import configparser

from functools import partial

def wmftimestamp(bytestring):
    s = bytestring.decode('utf-8')
    return dt.strptime(s, '%Y%m%d%H%M%S')

def decode_or_nouser(b):
    if isinstance(b, bytes):
        return b.decode('utf-8') if b else '#nosuchuser'
    else:
        return b

## Functions to get a retrieve user histories because SQL to slow and
## data too large to do it on SQL server.

datadir = ''
db_prefix = ''
userhistlist = ''
con = None
thank_df = None

thankcache = {}
thankcachemp = {}
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

def warm_thankcachemp():
    users = set()
    users.update(thank_df['receiver'])
    users.update(thank_df['sender'])
    print(f'found {len(users)} users')
    for role in ['receiver', 'sender']:
        for user in users:
            cachekey = f'{user}_{role}'
            user_cond =  (thank_df[role] == user)
            df = thank_df[user_cond]
            thankcachemp[cachekey] = df
    print(f'len of thankcachemp is {len(thankcachemp)}')

def thank_row(row, first_role, second_role):
    user = row[first_role]
    timestamp = row['timestamp']
    cachekey = f'{user}_{second_role}'
    try:
        df = thankcachemp[cachekey]
    except KeyError:
        user_cond =  (thank_df[second_role] == user)
        df = thank_df[user_cond]
        thankcachemp[cachekey] = df
        
    return df[:timestamp].shape[0] -1 #minus one because we want exclusive range


def thank_another_row(row, future):
    user = row['receiver']
    timestamp = row['timestamp']
    cachekey = f'{user}_sender'
    try:
        df = thankcachemp[cachekey]
    except KeyError:
        user_cond =  (thank_df['sender'] == user)
        df = thank_df[user_cond]
        thankcachemp[cachekey] = df

    high_end = timestamp + td(days=future)
    return df[timestamp:high_end].shape[0] -1 #minus one because


rpr = partial(thank_row, first_role='receiver', second_role='receiver')
rps = partial(thank_row, first_role='receiver', second_role='sender')
spr = partial(thank_row, first_role='sender', second_role='receiver')
sps = partial(thank_row, first_role='sender', second_role='sender')

rta1 = partial(thank_another_row, future=1)
rta30 = partial(thank_another_row, future=30)
rta90 = partial(thank_another_row, future=90)
rta180 = partial(thank_another_row, future=180)

missing_users = set()

def load_userid_df(userid):
    '''returns none if userid is not found'''
    try:
        pickle_loc = os.path.join(datadir, 'user_histories', '{}.pickle.index'.format(userid))
        df = pd.read_pickle(pickle_loc)
        return df, True
    except FileNotFoundError:
        missing_users.add(userid)
        # usercache[userid] = float(nan) # don't cache to save on headaches
        return pd.DataFrame(columns=['rev_timestamp']).set_index('rev_timestamp',drop=False), False


usercache = {}
usercachemp = {}
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

def warm_usercachemp(userids):
    print(f'found {len(userids)} userids')
    for userid in userids:
        df, user_exists = load_userid_df(userid)
        usercachemp[userid] = df

def edits_row(row, role_id):
    '''edits befor timestamp'''
    timestamp = row['timestamp']
    userid = row[role_id]
    try:
        df = usercachemp[userid] 
    except KeyError:
        df, user_exists = load_userid_df(userid)
        usercachemp[userid] = df

    return df[:timestamp].shape[0] -1 #exclusive

def edits_after_row(row, role_id, future):
    '''edits befor timestamp'''
    timestamp = row['timestamp']
    userid = row[role_id]
    try:
        df = usercachemp[userid] 
    except KeyError:
        df, user_exists = load_userid_df(userid)
        usercachemp[userid] = df
        
    high_end = timestamp + td(days=future)
    return df[timestamp:high_end].shape[0] -1 #exclisve

rpe = partial(edits_row, role_id='receiver_id')
spe = partial(edits_row, role_id='sender_id')

re1 = partial(edits_after_row, role_id='receiver_id', future=1)
re30 = partial(edits_after_row, role_id='receiver_id', future=30)
re90 = partial(edits_after_row, role_id='receiver_id', future=90)
re180 = partial(edits_after_row, role_id='receiver_id', future=180)
se1 = partial(edits_after_row, role_id='sender_id', future=1)
se30 = partial(edits_after_row, role_id='sender_id', future=30)
se90 = partial(edits_after_row, role_id='sender_id', future=90)
se180 = partial(edits_after_row, role_id='sender_id', future=180)


def _apply_df(args):
    df, func, kwargs = args
    # print(f'df: {len(df)}. func:{func}. kwags:{kwargs}')
    return df.apply(func, **kwargs)

def apply_mp(df, func,  max_workers=None, **kwargs):
    workers = cpu_count() if max_workers is None else max_workers
    pool = multiprocessing.Pool(processes=workers)
    result = pool.map(_apply_df, [(d, func, kwargs)
                                  for d in np.array_split(df, workers)])
    pool.close()
    return pd.concat(list(result))


def proc_user(user_id):
#     print('doing {}'.format(user_id))
    # check that a user id was able to be found
    if user_id >= 0:
    #     print('working on {}'.format(user))
        pickle_filename = '{}.pickle.index'.format(user_id)
        if not pickle_filename in userhistlist:
            sql = f'''
            select rev_timestamp from {db_prefix}.revision_userindex r
            where rev_user = {user_id}'''
            #print(rev_user_sql)
            MAXRETRIES = 4
            retries = 0
            while (retries < MAXRETRIES):
                sleep(retries**2)
                sleep(random.random()) #sleep a random amount to help with multiprocessing
                try:
                    df = pd.read_sql(sql, con)
                    df['rev_timestamp'] = df['rev_timestamp'].apply(wmftimestamp)
                    print('{} df has length {}'.format(user_id, len(df)))
                    pickle_path = os.path.join(datadir, 'user_histories', pickle_filename)
                    df.to_pickle(pickle_path)
                    return True
                except Exception as e:
                    print(f'SQL query is {sql}')
                    print(f'Exception is {e}')
                    retries += 1
            return False



        else:
            sys.stdout.write('')#make '.' if yuo want to see progress
            pass
    #this corresontp
    else:
        print('user id less than 0')
        pass


def make_lang(langcode, love_thank, test_run=False, dump_thank_df=False, load_thank_df=False):

    # i hate using globals, but because of multiprocess headaches this might be simple
    global datadir
    datadir = os.path.join('/data/project/civilservant_wiki_gratitude/data', langcode)
    print(f'datadir is : {datadir}')
    global db_prefix
    db_prefix = '{}wiki_p'.format(langcode)
    global thank_df

    # test if we're already done.
    outputdir = os.path.join(datadir, 'outputs')
    os.makedirs(outputdir, exist_ok=True)
    todaystr = dt.today().strftime('%Y%m%d')
    outfilestart = f'wiki{love_thank}_{langcode}'
    outfilecompl = f'{outfilestart}_{todaystr}.csv'
    outfile =  os.path.join(outputdir, outfilecompl)
    outputlist = os.listdir(outputdir)
    if outfilecompl in outputlist:
        #we've already donet this recently enough
        print(f'outfilestart is {outfilestart}.  output dir list is: {outputlist}')
        print('but im passing')
        #return True
        pass

    site = mwclient.Site(('https', f'{langcode}.wikipedia.org'), path = '/w/')

    os.makedirs(datadir, exist_ok=True)

    if not load_thank_df:
        os.environ['MYSQL_CATALOG'] = 'DB'
        replica_file = os.path.expanduser('~/replica.my.cnf')
        if os.path.isfile(replica_file):
            #just shoehore this in here if we're on a VPS
            cnf = configparser.ConfigParser()
            cnf.read_file(open(replica_file, 'r'))
            os.environ['MYSQL_USERNAME'] = cnf.get('client','user').replace("'","")
            os.environ['MYSQL_PASSWORD'] = cnf.get('client','password').replace("'","")
            os.environ['MYSQL_HOST'] = f'{langcode}wiki.analytics.db.svc.eqiad.wmflabs'
            os.environ['MYSQL_CATALOG'] = db_prefix

        constr = 'mysql+pymysql://{user}:{pwd}@{host}/{catalog}?charset=utf8'.format(user=os.environ['MYSQL_USERNAME'],
                                                              pwd=os.environ['MYSQL_PASSWORD'],
                                                              host=os.environ['MYSQL_HOST'],
                                                              catalog=os.environ['MYSQL_CATALOG'])
        print(f'constring is: {constr}')
        global con
        con = create_engine(constr, encoding='utf-8')

    #     con.execute(f'use {db_prefix};')

        thanks_sql = f"""select timestamp,
                receiver,
                ru.user_id as receiver_id,
                sender,
                su.user_id as sender_id
        from
        (select log_timestamp as timestamp, replace(log_title, '_', ' ') as receiver, log_user_text as sender from {db_prefix}.logging where log_type = 'thanks') t
        left join {db_prefix}.user ru on ru.user_name = t.receiver
        left join {db_prefix}.user su on su.user_name = t.sender
        where
        timestamp < 20180601000000
        """

        love_sql = f"""select wll_timestamp as timestamp,
        wll_receiver as receiver,
        wll_receiver as receiver_id,
        wll_sender as sender,
        wll_sender as sender_id,
        wll_type
        from {db_prefix}.wikilove_log
        where
        wll_timestamp < 20180601000000
        """

        which_sql_dict = {'thank':thanks_sql,
                           'love':love_sql}

        which_sql = which_sql_dict[love_thank]

        thank_df = pd.read_sql(which_sql, con)


        thank_df['receiver'] = thank_df['receiver'].apply(decode_or_nouser)
        thank_df['sender'] = thank_df['sender'].apply(decode_or_nouser)
        thank_df['timestamp'] = thank_df['timestamp'].apply(wmftimestamp)

        if love_thank == 'love':
            thank_df['wll_type'] = thank_df['wll_type'].apply(decode_or_nouser)

        print('#####')
        print(f'love_thank is {love_thank}')
        print(thank_df.head())

        ## Shorten the dataframe if we're testing
        if test_run:
            thank_df_full = thank_df
            thank_df = thank_df_full[:100] #three forty because that's the min of hte things we're looking at



        ## Get changed name ids
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
            elif len(sen_user_df) >0:
                user_id = sen_user_df['sender_id'].values[0]
                return user_id
            else:
                #TODO
                #we'd have to go make a seperate sql query for this
                return -1

        for direction in ['sender','receiver']:
            for oldname, newname in actual_moves.items():
                user_id = get_id(newname)
                print(f'going to replace {newname} with {user_id}')
                thank_df.loc[thank_df[direction] == oldname, f'{direction}_id'] = user_id
                

        ## cast everything back to ints
        thank_df['receiver_id'] = thank_df['receiver_id'].fillna(-1).astype(int)
        thank_df['sender_id'] = thank_df['sender_id'].fillna(-1).astype(int)

        user_ids = set()
        user_ids.update(thank_df['receiver_id'].values)
        user_ids.update(thank_df['sender_id'].values)

        print(f'we found {len(user_ids)} user ids for which to get history')
        userhistdir = os.path.join(datadir,'user_histories')
        os.makedirs(userhistdir, exist_ok=True)
        print(f'using user history directory {userhistdir}')
        global userhistlist
        userhistlist = os.listdir(userhistdir)
        

        
        #gymnastic to tryin to keep functional with the multiprocessing requiremnet that functions live in root namespace
        # user_ids_withdir = [(u, userhistlist, db_prefix, con) for u in user_ids]
        
        with Pool(10) as p:
            res = p.map_async(proc_user, user_ids)
            res.get()
            
        print('all done getting user history')
            
            

    if dump_thank_df:
        thank_df.to_pickle('data/en/outputs/thank_df.pickle')
    else:
        pass

    #otherwise we are loading thank df
    else:
        assert load_thank_df
        thank_df = pd.read_pickle(os.path.join(datadir, 'outputs/thank_df.pickle'))
        if test_run:
            thank_df = thank_df[:test_run]


    firsteditcache = {}
    def first_edit(userid):
        if not userid in firsteditcache.keys():
            df, user_exists = load_userid_df(userid)
            if not user_exists:
                return float('nan')
            else:
                mindate = df.index.min()
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

    # Add datetime index
    print('creating and sorting index')
    i1 = time.time()
    thank_df = thank_df.set_index('timestamp', drop=False)
    i2 = time.time()
    thank_df = thank_df.sort_index()
    i3 = time.time()
    print(f'make index took: {i2-i1}')
    print(f'sort index took: {i3-i2}')

    # # warm cache
    # print('warming caches')
    # w1 = time.time()
    # top_receivers = thank_df['receiver_id'].value_counts()[:1000].index
    # warm_usercachemp(top_receivers)
    # w2 = time.time()
    # print(f'warm usercache took {w2-w1}')
    # print(f'warm usercache has size {sys.getsizeof(usercachemp)}')


    # sort it to make cacheing strategy faster
    print('now sorting by receiver id')
    s1 = time.time()
    thank_df = thank_df.sort_values(by='receiver_id')
    s2 = time.time()
    print(f'sorting receiver id took {s2-s1}')

    # play around with max workers if multiprocessing makes you run out of ram somehow
    max_workers = 5

    print('computing rpe')
    # t5 = time.time()
    # thank_df['receiver_prev_edits_slow'] = thank_df.apply(lambda row: num_prev_edits(userid=row[2], prior_to=row[0]), axis=1) 
    t6 = time.time()
    thank_df['receiver_prev_edits'] = apply_mp(thank_df, rpe, max_workers, axis=1)
    t7 = time.time()
    # if not all(thank_df['receiver_prev_edits_slow']==thank_df['receiver_prev_edits']):
    #    print(thank_df[thank_df['receiver_prev_edits_slow']!=thank_df['receiver_prev_edits']][['receiver_prev_edits_slow','receiver_prev_edits']])
    # print(f'rpe slow: {t6-t5}')
    print(f'rpe fast: {t7-t6}')

    print('computing spe')
    # thank_df['sender_prev_edits_slow'] = thank_df.apply(lambda row: num_prev_edits(userid=row[4], prior_to=row[0]), axis=1)
    thank_df['sender_prev_edits'] = apply_mp(thank_df, spe, max_workers, axis=1)

    print('computing first edits')
    thank_df['sender_first_edit'] = thank_df['sender_id'].apply(first_edit)
    thank_df['receiver_first_edit'] = thank_df['receiver_id'].apply(first_edit)

    print('computing se1d')
    # t8 = time.time()
    # thank_df['sender_# edits_1d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=1), axis=1)
    t9 = time.time()
    thank_df['sender_edits_1d_after'] = apply_mp(thank_df, se1, max_workers, axis=1)
    t10 = time.time()
    # if not all(thank_df['sender_edits_1d_after_slow']==thank_df['sender_edits_1d_after']):
    #    print(thank_df[thank_df['sender_edits_1d_after_slow']!=thank_df['sender_edits_1d_after']][['sender_edits_1d_after_slow','sender_edits_1d_after']])
    # print(f'rpe slow: {t9-t8}')
    print(f'se1 fast: {t10-t9}')
    
    # print('computing se30d')
    # thank_df['sender_edits_30d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=30), axis=1)
    # print('computing se90d')
    # thank_df['sender_edits_90d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=90), axis=1)
    # print('computing se180d')
    # thank_df['sender_edits_180d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[4], timestamp=row[0], future=180), axis=1)
    # print('computing re1d')
    # thank_df['receiver_edits_1d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=1), axis=1)
    # print('computing re30d')
    # thank_df['receiver_edits_30d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=30), axis=1)
    # print('computing re90d')
    # thank_df['receiver_edits_90d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=90), axis=1)
    # print('computing re180d')
    # thank_df['receiver_edits_180d_after_slow'] = thank_df.apply(lambda row: num_edits_during(userid=row[2], timestamp=row[0], future=180), axis=1)

    print('computing se30d')
    thank_df['sender_edits_30d_after'] = apply_mp(thank_df, se30, max_workers, axis=1)
    print('computing se90d')
    thank_df['sender_edits_90d_after'] = apply_mp(thank_df, se90, max_workers, axis=1)
    print('computing se180d')
    thank_df['sender_edits_180d_after'] = apply_mp(thank_df, se180, max_workers, axis=1)
    print('computing re1d')
    thank_df['receiver_edits_1d_after'] = apply_mp(thank_df, re1, max_workers, axis=1)
    print('computing re30d')
    thank_df['receiver_edits_30d_after'] = apply_mp(thank_df, re30, max_workers, axis=1)
    print('computing re90d')
    thank_df['receiver_edits_90d_after'] = apply_mp(thank_df, re90, max_workers, axis=1)
    print('computing re180d')
    thank_df['receiver_edits_180d_after'] = apply_mp(thank_df, re180, max_workers, axis=1)



    # # t1 = time.time()
    # thank_df['receiver_prev_received_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='receiver', timestamp=row[0], future=None), axis=1)
    # t2 = time.time()
    t3 = time.time()
    print('computing rpr: c')
    thank_df['receiver_prev_received'] = apply_mp(thank_df, rpr, axis=1)
    t4 = time.time()
    # print(f'way a took: {t2-t1}')

    print(f'way c took: {t4-t3}')
    # compac = thank_df['receiver_prev_received']!=thank_df['receiver_prev_received_slow']
    # if any(compac):
    #     print(f"not the same")
    #     print(thank_df[compac][['receiver_prev_received', 'receiver_prev_received_slow']].head().to_string())
    # else:
    #     print('the same')

    print('computing rps')
    # thank_df['receiver_prev_sent_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=None), axis=1)
    thank_df['receiver_prev_sent'] = apply_mp(thank_df, rps, axis=1)
    # if not all(thank_df['receiver_prev_sent_slow']==thank_df['receiver_prev_sent']):
    #     print('rps error')

    print('computing spr')
    # thank_df['sender_prev_received_slow'] = thank_df.apply(lambda row: thank_another(user=row[3], role='receiver', timestamp=row[0], future=None), axis=1)
    thank_df['sender_prev_received'] = apply_mp(thank_df, spr, axis=1)

    print('computing sps')
    # thank_df['sender_prev_sent'] = thank_df.apply(lambda row: thank_another(user=row[3], role='sender', timestamp=row[0], future=None), axis=1)
    thank_df['sender_prev_sent'] = apply_mp(thank_df, sps, axis=1)


    print('computing indicators')
    conticols = ["receiver_prev_received","sender_prev_received","sender_prev_sent","receiver_prev_sent"]
    for col in conticols:
        indcol = "{col}_indicator".format(col=col)
        thank_df[indcol] = thank_df[col].apply(lambda x: x>0)

    print('computing rta1d')
    # thank_df['receiver_thank_another_1d_after_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=1), axis=1)
    thank_df['receiver_thank_another_1d_after'] =  apply_mp(thank_df, rta1, axis=1)

    print('computing rta30d')
    # thank_df['receiver_thank_another_30d_after_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=30), axis=1)
    thank_df['receiver_thank_another_30d_after'] =  apply_mp(thank_df, rta30, axis=1)

    print('computing rta90d')
    # thank_df['receiver_thank_another_90d_after_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=90), axis=1)
    thank_df['receiver_thank_another_90d_after'] =  apply_mp(thank_df, rta90, axis=1)

    print('computing rta180d') 
    # thank_df['receiver_thank_another_180d_after_slow'] = thank_df.apply(lambda row: thank_another(user=row[1], role='sender', timestamp=row[0], future=180), axis=1) 
    thank_df['receiver_thank_another_180d_after'] =  apply_mp(thank_df, rta180, axis=1)



    # assert all(thank_df['receiver_thank_another_1d_after_slow']==thank_df['receiver_thank_another_1d_after'])
    # assert all(thank_df['receiver_thank_another_30d_after_slow']==thank_df['receiver_thank_another_30d_after'])

    
    

    # TESTS


    # def natural_integer_test(seq):
    #     '''tests if a series is a natural integer sequence using a property'''
    #     l = len(seq) - 1 #minus 1 because we always start with a zero
    #     sum_identity = (l*(l+1))/2
    #     assert sum_identity == seq.sum()

    # def monotonic(x):
    #     if np.any(np.isnan(x)):
    #         return True
    #     else:
    #         assert np.all(np.diff(x) >= 0)

    # most_sender = thank_df['sender'].value_counts().index[0]

    # most_sender_r = thank_df[thank_df['receiver']==most_sender]
    # most_sender_s = thank_df[thank_df['sender']==most_sender]
    # most_sender_rpr = most_sender_r['receiver_prev_received']
    # most_sender_rps = most_sender_r['receiver_prev_sent']
    # most_sender_sps = most_sender_s['sender_prev_sent']
    # most_sender_spr = most_sender_s['sender_prev_received']

    # #natural_integer_test(most_sender_rpr)
    # #natural_integer_test(most_sender_sps)
    # monotonic(most_sender_spr)
    # monotonic(most_sender_rps)


    # for usercol, featcol in (('sender', 'sender_prev_edits'), ('receiver','receiver_prev_edits')):
    #     print(usercol, featcol)
    #     for name, group in thank_df.groupby(usercol):
    #         monotonic(group[featcol])
    # print('all clear')



    # for name, group in thank_df.groupby('sender'):
    #     assert len(group['sender_first_edit'].unique()) == 1
    # print('all tests passed')

    ### END TESTS

    outputdir = os.path.join(datadir, 'outputs')
    os.makedirs(outputdir, exist_ok=True)
    todaystr = dt.today().strftime('%Y%m%d')
    outfile = os.path.join(outputdir, f'wiki{love_thank}_{langcode}_{todaystr}.csv')
    print(f'Saving file to {outfile}')
    thank_df.to_csv(outfile, index=False)

if __name__ == '__main__':
    @click.command()
    @click.option('--conf', default='test',
              help='the json file to look for in configs without `.json`')
    def read_conf(conf):
        print(f'running with conf {conf}')
        configs = json.load(open(os.path.join('configs', f'{conf}.json'),'r'))
        test_run = False
        dump_thank_df = False
        load_thank_df = False
        just_love = False
        if 'test_run' in configs.keys():
            test_run = configs['test_run']
        if 'dump_thank_df' in configs.keys():
            dump_thank_df = configs['dump_thank_df']
        if 'load_thank_df' in configs.keys():
            load_thank_df = configs['load_thank_df']
        if 'just_love' in configs.keys():
            just_love = configs['just_love']

        for langcode in configs['langcodes']:
            for love_thank in ['thank', 'love']:
                if love_thank == 'thank' and just_love is True:
                    continue
                print(f'Now kicking off for: {langcode}. \n Love or thank? {love_thank}. \n Test run?: {test_run}')
                print('################')
                MAXOUTERLOOPRETRIES = 1
                outerloopretry = 0
                while outerloopretry < MAXOUTERLOOPRETRIES:
                    try:
                        make_lang(langcode, love_thank=love_thank,
                                  test_run=test_run,
                                  dump_thank_df=dump_thank_df,
                                  load_thank_df=load_thank_df)
                        break
                    except Exception as e:
                        raise
                        print(f'outerloopexecption is {e}')
                        print(f'retry number {outerloopretry}')
                        outerloopretry += 1
                        sleep(outerloopretry**2)

    #and finall run it
    read_conf()
