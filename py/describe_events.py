from collections import Counter
from itertools import chain
from functools import wraps
import numpy as np
import psycopg2
import json
import os

keyword_target = 'hate'

filters = [
    'united states',
    'local news',
    'u.s. news',
    'president',
    'politics',
    'american',
    'america',
    'report',
    'donald',
    'trumps',
    'trump',
    'crimes',
    'crime',
    'groups',
    'group',
    'hates',
    'hated',
    'hater',
    'hate',
    'news'
]

np.seterr('ignore')


def get_config(in_file):

    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict


def get_connection():

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host)


def open_connection(f):
    @wraps(f)
    def wrap(*args, **kw):
        with get_connection() as con:
            with con.cursor() as cur:
                res = f(cur, *args, **kw)
                return res
    return wrap


def get_event_info(event_data):

    event_boundaries = {}

    start_idx = None

    for l_idx, l_row in enumerate(event_data):

        if start_idx and l_idx < start_idx: continue

        try:
            l_date, l_articles = l_row[0], l_row[1]

            three_avg = np.mean([r[1] for r in event_data[l_idx - 3: l_idx]])

            if abs(l_articles - three_avg) >= 3 * three_avg:
                for r_idx, r_row in enumerate(event_data[l_idx + 1:], start=l_idx + 1):

                    r_date, r_articles = r_row[0], r_row[1]

                    if r_articles <= (event_data[l_idx - 1][1]):

                        start_idx = r_idx
                        bound_key = '{}_{}'.format(event_data[l_idx - 1][0], event_data[r_idx][0])

                        event_boundaries.update({bound_key: {'length': r_idx - (l_idx - 2)}})

                        break

        except KeyError:
            pass

    return event_boundaries


@open_connection
def get_event_data(cursor):

    the_sql = '''
              select sqldate, sum(numarticles::integer) 
              from gdelt_{0}
              group by sqldate order by sqldate
              '''.format(keyword_target)

    cursor.execute(the_sql)

    return [r for r in cursor.fetchall()]


@open_connection
def get_extent_area(cursor, event_range):

    the_sql = '''
              select round(st_area(st_convexhull(st_collect(geom))))
              from gdelt_{0}
              where sqldate between '{1}' and '{2}'
              '''.format(keyword_target, *event_range)

    cursor.execute(the_sql)

    return cursor.fetchall()[0][0]


@open_connection
def get_event_actors(cursor, event_range):

    the_sql = '''
              select sqldate, array_agg(actor1name), array_agg(actor2name), sum(numarticles) as articles 
              from gdelt_{0}
              where sqldate between '{1}' and '{2}'
              group by sqldate order by articles desc
              '''.format(keyword_target, *event_range)

    cursor.execute(the_sql)
    result_set = cursor.fetchall()

    a1 = set()
    a2 = set()

    for result in result_set:

        for r in Counter(result[1]).most_common(10):
            a1.add(r[0])

        for r in Counter(result[2]).most_common(10):
            a2.add(r[0])

    a1_c = [c[0] for c in Counter(a1).most_common(5)]
    a2_c = [c[0] for c in Counter(a2).most_common(5)]
    return list(a1_c), list(a2_c)


@open_connection
def get_event_keys(cursor, event_range):

    the_sql = '''
              select sqldate, array_agg(keywords), array_agg(meta_keys) from gdelt_{0}
              where sqldate between '{1}' and '{2}'
              group by sqldate order by sqldate
              '''.format(keyword_target, *event_range)

    cursor.execute(the_sql)

    common_keys = []

    for row in cursor.fetchall():

        keys = list(chain(*[k.split(',') for k in row[1] if k]))
        keys = [k.strip().lower() for k in keys if k]
        keys = [k for k in keys if k not in filters]

        for c in [c[0] for c in Counter(keys).most_common(10)]:
            common_keys.append(c)

    return [c[0] for c in Counter(common_keys).most_common(5)]


@open_connection
def get_event_tone(cursor, event_range):

    the_sql = '''
              select round(avg(avgtone::float)) from gdelt_{0}
              where sqldate between '{1}' and '{2}'
              group by sqldate order by sqldate
              '''.format(keyword_target, *event_range)

    cursor.execute(the_sql)

    return round(np.mean([r[0] for r in cursor.fetchall()]))


@open_connection
def get_event_articles(cursor, event_range):

    the_sql = '''
              select sum(numarticles::integer) from gdelt_{0}
              where sqldate between '{1}' and '{2}'
              '''.format(keyword_target, *event_range)

    cursor.execute(the_sql)

    return cursor.fetchall()[0][0]


if __name__ == "__main__":

    # Get Project Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Collect Config Parameters
    params  = get_config(os.path.join(this_dir, 'config.json'))
    db_name = params['db_name']
    db_user = params['db_user']
    db_pass = params['db_pass']
    db_host = params['db_host']

    all_events = {}

    event_data = get_event_data()
    event_info = get_event_info(event_data)

    for event_range in event_info.keys():

        print('Running: {}'.format(event_range))

        er = event_range.split('_')

        # Collect Actors
        a1_top, a2_top = get_event_actors(er)
        event_info[event_range].update({'actor_one': a1_top, 'actor_two': a2_top})

        # Collect Tone
        tone = get_event_tone(er)
        event_info[event_range].update({'tone': tone})

        # Collect Area
        area = get_extent_area(er)
        event_info[event_range].update({'area': area})

        # Collect Keywords
        keywords = get_event_keys(er)
        event_info[event_range].update({'keywords': keywords})

        # Collect Articles
        articles = get_event_articles(er)
        event_info[event_range].update({'articles': articles})

        all_events.update(event_info)

    with open('../planning/gdelt_{0}.json'.format(keyword_target), 'w') as the_file:
        json.dump(all_events, the_file, indent=2)
