from collections import Counter
from itertools import chain
from functools import wraps
import psycopg2
import warnings
import datetime
import numpy
import arcpy
import json
import time
import os

warnings.filterwarnings('ignore')

filters = [
    'united states',
    'president',
    'politics',
    'american',
    'america',

    'local news',
    'u.s. news',
    'report',
    'media',
    'news',

    'donald',
    'trumps',
    'trump',

    'crimes',
    'crime',

    'groups',
    'group',

    'hateful',
    'hates',
    'hated',
    'hater',
    'hate',

    'say',
    'told',
    'attack',
    'murder',
    'charged',
    'york',
    'incidents'
]


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

        if start_idx and l_idx < start_idx:
            continue

        try:
            l_date, l_articles = l_row[0], l_row[1]

            three_avg = numpy.mean([r[1] for r in event_data[l_idx - 3: l_idx]])

            if l_articles >= 3 * three_avg:

                for r_idx, r_row in enumerate(event_data[l_idx + 1:], start=l_idx + 1):

                    r_date, r_articles = r_row[0], r_row[1]

                    l_articles = event_data[l_idx - 1][1]
                    l_articles = l_articles if l_articles > 20 else 25

                    if r_articles <= l_articles + 10:

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
              select dateadded, sum(numarticles::integer) 
              from gdelt_hate
              group by dateadded order by dateadded
              '''

    cursor.execute(the_sql)

    return [r for r in cursor.fetchall()]


@open_connection
def get_event_actors(cursor, event_range):

    the_sql = '''
              select actor1name, actor2name
              from gdelt_hate
              where dateadded between '{}' and '{}'
              order by numarticles desc
              limit 5
              '''.format(*event_range)

    cursor.execute(the_sql)
    results = [[*r] for r in cursor.fetchall()]

    a1 = [r[0] for r in results]
    a2 = [r[1] for r in results]

    return a1, a2


@open_connection
def get_event_keys(cursor, event_range):

    the_sql = '''
              select dateadded, array_agg(keywords) from gdelt_hate
              where dateadded between '{}' and '{}'
              group by dateadded order by dateadded
              '''.format(*event_range)

    cursor.execute(the_sql)

    common_keys = []
    for row in cursor.fetchall():

        keys = list(chain(*[k.split(',') for k in row[1] if k]))
        keys = [k.strip().lower() for k in keys if k]
        keys = [k for k in keys if k not in filters]

        for key in keys:
            common_keys.append(key)

    key_cnt = [c[1] for c in Counter(common_keys).most_common(15) if c[1] >= 3]
    key_set = sorted(list(set(key_cnt)), reverse=True)

    if len(key_set) > 5:
        valid_counts = key_set[:5]
        return [c[0].upper() for c in Counter(common_keys).most_common(15) if c[1] in valid_counts]

    else:
        return []


@open_connection
def get_event_tone(cursor, event_range):

    the_sql = '''
              select round(avg(avgtone::float)) from gdelt_hate
              where dateadded between '{}' and '{}'
              group by dateadded order by dateadded
              '''.format(*event_range)

    cursor.execute(the_sql)

    return round(numpy.mean([r[0] for r in cursor.fetchall()]))


@open_connection
def get_division_counts(cursor, event_range):

    the_sql = '''
              select wc from window_counts
              where dateadded = '{}'
              '''.format(event_range)

    cursor.execute(the_sql)

    the_count = cursor.fetchall()[0][0]

    return the_count


@open_connection
def get_event_articles(cursor, event_range):

    the_sql = '''
              select sum(numarticles::integer) from gdelt_hate
              where dateadded between '{}' and '{}'
              '''.format(*event_range)

    cursor.execute(the_sql)

    return cursor.fetchall()[0][0]


@open_connection
def get_event_peak(cursor, event_range):

    the_sql = '''
              select dateadded, sum(numarticles::integer) as a_c
              from gdelt_hate where dateadded between '{}' and '{}'
              group by dateadded order by a_c desc limit 1
              '''.format(*event_range)

    cursor.execute(the_sql)

    peak_date  = datetime.datetime.strptime(cursor.fetchall()[0][0], '%Y%m%d')

    first_date = datetime.datetime.strptime(event_range[0], '%Y%m%d')
    last_date  = datetime.datetime.strptime(event_range[1], '%Y%m%d')

    ascent     = peak_date - first_date
    descent    = last_date - peak_date

    return ascent.days, descent.days


@open_connection
def get_date_info(cursor, event_range):

    the_sql = '''
              select dateadded, sum(numarticles)
              from gdelt_hate
              where dateadded between '{}' and '{}'
              group by dateadded order by dateadded
              '''.format(*event_range)

    cursor.execute(the_sql)
    records = cursor.fetchall()

    return [r for r in records]


def build_window_info():

    print('Building Event Overviews')

    all_events = {}
    del_events = []

    event_data = get_event_data()
    event_info = get_event_info(event_data)

    for event_range in event_info.keys():

        er = event_range.split('_')

        # Collect Keywords
        keywords = get_event_keys(er)
        if not keywords:
            del_events.append(event_range)
        event_info[event_range].update({'keywords': keywords})

        # Collect Articles
        articles = get_event_articles(er)
        event_info[event_range].update({'articles': articles})

        # Get Number of Days from Peak to End
        ascent, descent = get_event_peak(er)
        event_info[event_range].update({'ascent': ascent, 'descent': descent})

        # Collect Tone
        tone = get_event_tone(er)
        event_info[event_range].update({'tone': tone})

        # # Collect Actors
        # a1_top, a2_top = get_event_actors(er)
        # event_info[event_range].update({'actor_one': a1_top, 'actor_two': a2_top})

        # # Collect Division Counts
        # division_count = get_division_counts(event_range)
        # event_info[event_range].update({'div_cnt': division_count})

        all_events.update(event_info)

    for event in del_events: del all_events[event]

    return all_events


def process_window(er, attributes, hate_fc, windows):

    try:
        fl = arcpy.MakeFeatureLayer_management(
            hate_fc,
            os.path.join(arcpy.env.scratchGDB, 'WINDOW_{}_{}'.format(*er)),
            where_clause="dateadded between '{}' and '{}'".format(*er)
        )

        dd = arcpy.DirectionalDistribution_stats(
            fl,
            os.path.join(windows, 'WINDOW_{}_{}'.format(*er)),
            '1_STANDARD_DEVIATION',
            'numarticles'
        )

        # Create Additional Fields for Attribute Insertion
        arcpy.AddField_management(dd, 'START_DATE', 'DATE')
        arcpy.AddField_management(dd, 'END_DATE',   'DATE')
        arcpy.AddField_management(dd, 'ARTICLES',   'LONG')
        arcpy.AddField_management(dd, 'WIN_LEN',    'LONG')
        for idx, key in enumerate(attributes['keywords'], start=1):
            arcpy.AddField_management(dd, 'KEY_{}'.format(idx), 'TEXT')

        # Set Field to be Handled by Update Cursor
        key_fields = ['KEY_{}'.format(k) for k in [a for a in range(1, len(attributes['keywords']) + 1)]]
        all_fields = ['START_DATE', 'END_DATE', 'ARTICLES', 'WIN_LEN'] + key_fields

        start_date = datetime.datetime.strptime(er[0], '%Y%m%d')
        end_date   = datetime.datetime.strptime(er[1], '%Y%m%d')
        articles   = attributes['articles']
        win_len    = attributes['length']
        key_values = [k for k in attributes['keywords']]

        with arcpy.da.UpdateCursor(dd, all_fields) as cursor:
            for _ in cursor:
                cursor.updateRow([start_date, end_date, articles, win_len] + key_values)

        return dd

    except arcpy.ExecuteError as a_e:
        print(a_e)


def process_dates(er, date_info, hate_fc, windows, length):

    mean_fcs = []
    date_fcs = []

    for date, articles in date_info:

        try:
            fl = arcpy.MakeFeatureLayer_management(
                hate_fc,
                os.path.join(arcpy.env.scratchGDB, 'GDELT_{}'.format(date)),
                where_clause="dateadded = '{}'".format(date)
            )
            fl_count = arcpy.GetCount_management(fl)[0]

            # Ignore Dates That Return Less Than 3 GDELT Records
            if int(fl_count) < 3:
                continue

            dd = arcpy.DirectionalDistribution_stats(
                fl,
                os.path.join(windows, 'D_{0}_{1}_{2}'.format(*er, date)),
                '1_STANDARD_DEVIATION',
                'numarticles'
            )

            mc = arcpy.MeanCenter_stats(
                fl,
                os.path.join(windows, 'MC_{0}_{1}_{2}'.format(*er, date)),
                'numarticles'
            )

            arcpy.AddField_management(dd, 'EVENT_DATE', 'DATE')
            arcpy.AddField_management(mc, 'EVENT_DATE', 'DATE')

            arcpy.AddField_management(mc, 'ARTICLES', 'LONG')
            arcpy.AddField_management(dd, 'ARTICLES', 'LONG')

            for target in [dd, mc]:
                with arcpy.da.UpdateCursor(target, ['EVENT_DATE', 'ARTICLES']) as cursor:
                    for _ in cursor:
                        cursor.updateRow([
                            datetime.datetime.strptime(date, '%Y%m%d'),
                            articles
                        ])

            date_fcs.append(dd)
            mean_fcs.append(mc)

        except arcpy.ExecuteError:
            pass

    print('{} - {}: {} of {} Processed'.format(*er, len(date_fcs), len(date_info)))

    arcpy.Merge_management(
        date_fcs,
        os.path.join(windows, 'GDELT_{0}_{1}_Windows'.format(*er))
    )

    mc_merge = arcpy.Merge_management(
        mean_fcs,
        os.path.join(windows, 'GDELT_{0}_{1}_MC'.format(*er))
    )

    pl = arcpy.PointsToLine_management(
        mc_merge,
        os.path.join(windows, 'MC_{0}_{1}'.format(*er)),
        Sort_Field='EVENT_DATE'
    )

    arcpy.AddField_management(pl, 'EVENT_LEN', 'LONG')
    with arcpy.da.UpdateCursor(pl, ['EVENT_LEN']) as cursor:
        for _ in cursor:
            cursor.updateRow([length])

    for fc in date_fcs:
        arcpy.Delete_management(fc)

    for fc in mean_fcs:
        arcpy.Delete_management(fc)

    movement = [row[0].length for row in arcpy.da.SearchCursor(pl, ['SHAPE@'])]
    movement = round(movement[0]) if movement else 0

    return movement


def build_window_geom(events, hate_fc, windows):

    arcpy.env.overwriteOutput = True

    merge_windows = []

    for event_range, attributes in events.items():

        print(f'Processing: {event_range}')

        er = event_range.split('_')

        date_info = get_date_info(er)

        m_w = process_window(er, attributes, hate_fc, windows)
        merge_windows.append(m_w)

        movement = process_dates(er, date_info, hate_fc, windows, attributes['length'])

        events[event_range].update({'movement': movement})

    arcpy.Merge_management(
        merge_windows,
        os.path.join(windows, 'GDELT_Windows')
    )

    return events


if __name__ == "__main__":

    # Get the Start Time
    start_time = time.time()

    # Get Project Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Collect Config Parameters
    params  = get_config(os.path.join(this_dir, 'config.json'))
    db_name = params['db_name']
    db_user = params['db_user']
    db_pass = params['db_pass']
    db_host = params['db_host']
    hate_fc = params['hate_fc']
    windows = params['windows']

    # Build Dictionary with General Window Information
    events = build_window_info()

    # Build Geometries Attributes for Windows
    events = build_window_geom(events, hate_fc, windows)

    # Dump Event Dictionary to Local JSON
    with open('../planning/gdelt_hate.json', 'w') as the_file:
        json.dump(events, the_file, indent=2)

    # Check Run Length
    print('Process Ran: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
