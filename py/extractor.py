from newspaper import Article, ArticleException
from multiprocessing import Pool, cpu_count
from functools import wraps, partial
from urllib.parse import urlparse
import traceback
import datetime
import calendar
import psycopg2
import requests
import tempfile
import zipfile
import shutil
import json
import sys
import csv
import re
import os


class Extractor(object):

    def __init__(self, config):

        self.config = self.read_config(config)

        # Explicitly Set DB Parameters
        self.db_name = self.config['db_name']
        self.db_user = self.config['db_name']
        self.db_pass = self.config['db_pass']
        self.db_host = self.config['db_host']

    @staticmethod
    def read_config(config):

        try:
            return config if isinstance(config, dict) else json.load(open(config))

        except ValueError as val_err:
            print(f'Configuration Input "{config}" is Not Valid: {val_err}')
            sys.exit(1)

    @staticmethod
    def get_date_range(y, m):

        return [
            datetime.date(y, m, day).strftime('%Y%m%d') for day in range(1, calendar.monthrange(y, m)[1] + 1)
        ]

    @staticmethod
    def extract_daily_csv(target_date):

        # Pull CSV from GDELT Repository
        date_zip = '{}.export.CSV.zip'.format(target_date)
        event_url = 'http://data.gdeltproject.org/events/{}'.format(date_zip)
        response = requests.get(event_url, stream=True)

        if response.status_code != 200:
            return None

        # Dumpt to Local CSV
        temp_dir = tempfile.mkdtemp(dir=r'C:\Temp', prefix='{}_'.format(target_date))
        zip_file = '{}/{}.zip'.format(temp_dir, target_date)
        with open(zip_file, 'wb') as f: f.write(response.content)
        with zipfile.ZipFile(zip_file, 'r') as the_zip: the_zip.extractall(temp_dir)

        return '{}/{}.export.CSV'.format(temp_dir, target_date)

    @staticmethod
    def text_filter(text):

        return re.sub('[^a-zA-Z0-9 \n]', '', text)

    def get_connection(self):

        return psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_pass, host=self.db_host)

    def process_article(self, source_url):

        # Parse GDELT Source
        article = Article(source_url)
        article.download()
        article.parse()
        article.nlp()

        # Unpack Article Properties & Replace Special Characters
        title = self.text_filter(article.title)
        summary = '{} . . . '.format(self.text_filter(article.summary)[:500])
        keywords = ', '.join(sorted([self.text_filter(key) for key in article.keywords]))
        meta_keys = ', '.join(sorted([self.text_filter(key) for key in article.meta_keywords]))
        site = urlparse(article.source_url).netloc

        return [title, site, summary, keywords, meta_keys]

    def process_events(self, year, target_csv):

        # Tracking
        seen_urls = []
        proc_urls = 0

        # Extract Records
        with open(target_csv, newline='', encoding='utf8') as the_csv:

            the_reader = csv.reader(the_csv, delimiter='\t')

            for idx, row in enumerate(the_reader, start=1):

                # Pull Filter Attributes
                avg_tone = float(row[34])  # Average Tone
                src_url = row[57]  # Source URL
                a1_geo_lat = row[39]  # Latitude Check
                a1_gc = row[37]  # Actor1Geo_Country
                a2_geo_lat = row[39]  # Longitude Check
                a2_gc = row[44]  # Actor1Geo_Country

                try:
                    # TODO - Actor1Geo_Type in ('2', '3')
                    if all([v == 'US' for v in [a1_gc, a2_gc]]) \
                            and avg_tone < 0 \
                            and src_url not in seen_urls \
                            and all([a1_geo_lat, a2_geo_lat]):

                        # Extract NLP Values with Article
                        derived_attributes = self.process_article(src_url)

                        # Push Values into Master Table
                        with self.get_connection() as conn:
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

                except:
                    print(f'{traceback.format_exc()}')

                finally:
                    seen_urls.append(src_url)

    def process_day(self, year, the_day):

        print(f'Processing Day: {the_day}')

        # Download GDELT Records Locally for Processing
        daily_csv = self.extract_daily_csv(the_day)

        # Ignore Bad CSV Requests
        if not daily_csv: return

        # Collect Enriched Values & Push Into Table
        self.process_events(year, daily_csv)

        # Remove Temporary Directory
        shutil.rmtree(os.path.dirname(daily_csv))

    def run_month(self, month, year):

        date_range = self.get_date_range(year, month)

        # Create Pool & Run Records
        pool = Pool(processes=cpu_count() - 1)
        pool.map(partial(self.process_day, year), date_range)
        pool.close()
        pool.join()
