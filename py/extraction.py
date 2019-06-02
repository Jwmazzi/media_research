from newspaper import Article, ArticleException
from multiprocessing import Pool, cpu_count
from functools import wraps, partial
from urllib.parse import urlparse
from time import time
import requests
import argparse
import psycopg2
import datetime
import calendar
import tempfile
import zipfile
import shutil
import json
import csv
import re
import os


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts  = time()
        res = f(*args, **kw)
        te  = time()
        return [res, round((te-ts) / 60)]
    return wrap


def get_input():

    p = argparse.ArgumentParser(description="Extract & Enrich GDELT.")

    p.add_argument("-y", help="Target Year")
    p.add_argument("-m", help="Target Month")

    return p.parse_args()


def get_config(in_file):

    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict


def get_connection():

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host)


def get_processed_dates():

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                select eventdate from gdelt_tracking
                '''
            )
            return [d[0] for d in cur.fetchall()]


def text_filter(text):

    return re.sub('[^a-zA-Z0-9 \n\.]', '', text)


def extract_daily_csv(target_date):

    # Pull CSV from GDELT Repository
    date_zip = '{}.export.CSV.zip'.format(target_date)
    event_url = 'http://data.gdeltproject.org/events/{}'.format(date_zip)
    response = requests.get(event_url, stream=True)

    # Dumpt to Local CSV
    temp_dir = tempfile.mkdtemp(dir=r'C:\Temp', prefix='{}_'.format(target_date))
    zip_file = '{}/{}.zip'.format(temp_dir, target_date)
    with open(zip_file, 'wb') as f: f.write(response.content)
    with zipfile.ZipFile(zip_file, 'r') as the_zip: the_zip.extractall(temp_dir)

    return '{}/{}.export.CSV'.format(temp_dir, target_date)


def process_article(source_url):

    # Parse GDELT Source
    article = Article(source_url)
    article.download()
    article.parse()
    article.nlp()

    # Unpack Article Properties & Replace Special Characters
    title     = text_filter(article.title)
    summary   = '{} . . . '.format(text_filter(article.summary)[:500])
    keywords  = ', '.join(sorted([text_filter(key) for key in article.keywords]))
    meta_keys = ', '.join(sorted([text_filter(key) for key in article.meta_keywords]))
    site      = urlparse(article.source_url).netloc

    return [title, site, summary, keywords, meta_keys]


@timing
def process_events(year, target_csv):

    # Tracking
    seen_urls = []
    proc_urls = 0

    # Extract Records
    with open(target_csv, newline='', encoding='utf8') as the_csv:

        the_reader = csv.reader(the_csv, delimiter='\t')

        for idx, row in enumerate(the_reader, start=1):

            if idx % 10000 == 0:
                print('{} - {}'.format(os.path.basename(target_csv), idx))

            # Pull Filter Attributes
            avg_tone   = float(row[34])  # Average Tone
            src_url    = row[57]         # Source URL
            a1_geo_lat = row[39]         # Latitude Check
            a1_gc      = row[37]         # Actor1Geo_Country
            a2_geo_lat = row[39]         # Longitude Check
            a2_gc      = row[44]         # Actor1Geo_Country

            try:
                # TODO - Rethink/Refactor Filtering
                if all([v == 'US' for v in [a1_gc, a2_gc]]) \
                        and avg_tone < 0 \
                        and src_url not in seen_urls \
                        and all([a1_geo_lat, a2_geo_lat]):

                    # Extract NLP Values with Article
                    derived_attributes = process_article(src_url)

                    # Push Values into Master Table
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                '''
                                insert into gdelt_{}
                                values {}
                                '''.format(year, tuple(row + derived_attributes))
                            )

                    proc_urls += 1

            except ArticleException:
                pass

            except Exception as e:
                print('Error: {}'.format(str(e)))

            finally:
                seen_urls.append(src_url)

    return proc_urls


def process_day(year, the_day):

    print('Started Running: {}'.format(the_day))

    # Download GDELT Records Locally for Processing
    daily_csv = extract_daily_csv(the_day)

    try:
        # Build Enriched Values & Push Into Table
        process_res = process_events(year, daily_csv)

        # Push Values into Master Table
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    insert into gdelt_tracking
                    values {}
                    '''.format(tuple([the_day] + process_res))
                )

    except Exception as e:
        print('Error: {}'.format(str(e)))

    finally:
        # Remove Temporary Directory
        shutil.rmtree(os.path.dirname(daily_csv))


if __name__ == "__main__":

    # Get Project Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Collect Config Parameters
    params  = get_config(os.path.join(this_dir, 'config.json'))
    db_name = params['db_name']
    db_user = params['db_user']
    db_pass = params['db_pass']
    db_host = params['db_host']

    # Collect Incoming Command Line Arguments
    args  = get_input()
    y     = args.y
    m     = args.m

    # TODO - Update Logic for Command Line Execution
    # Check If GDELT_{YEAR} Exist & Create If Necessary
    # Fork - If Y/M Determine Dates & Proceed
    # Fork - If No Command Line Inputs, Process Yesterday (Assumes Scheduled Task Each Morning @ ~ 8 AM EST)

    # Determine Dates
    days = [
        datetime.date(y, m, day).strftime('%Y%m%d') for day in range(1, calendar.monthrange(y, m)[1] + 1)
    ]

    # Do Not Repeat Finished Dates
    processed = get_processed_dates()
    dates_list = [d for d in days if d not in processed]

    # Create Pool & Run Records
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(partial(process_day, y), dates_list)
    pool.close()
    pool.join()
