import sqlite3
import pandas as pd
import requests
from pathlib import Path

PAGEVIEWS_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/"
WIKIPEDIA_API_BASE_URL = "https://en.wikipedia.org/w/api.php"
RESERVED_NAMESPACES = ['User', 'Wikipedia', 'File', 'MediaWiki', 'Template',
                       'Help', 'Category', 'Portal', 'Draft', 'MOS', 
                       'TimedText', 'Module', 'Special', 'Media']

class WikipediaFetcher:
    def __init__(self, headers, mode='csv', db_path='wikipedia.db', csv_path='pageviews.csv'):
        self.headers = headers
        self.mode = mode
        if mode not in ['csv', 'sql']:
            raise ValueError("Invalid mode. Please specify 'csv' or 'sql'.")
        if mode == 'sql':
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            self.init_db()
        else:
            self.csv_path = Path(csv_path)

    def __del__(self):
        self.close_connection()

    def read_last_csv_row(self):
        with open(self.csv_path, 'rb') as file:
            file.seek(-2, 2)
            while file.read(1) != b'\n':
                file.seek(-2, 1)
            last_line = file.readline().decode('utf-8', errors='replace')  # Replace invalid chars
        return last_line.strip().split(',')


    def init_db(self):
        # Create articles table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS article (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL UNIQUE
        );
        """)

        # Create daily visits table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS pageview (
        date TEXT,
        article_id INTEGER,
        views INTEGER,
        rank INTEGER,
        FOREIGN KEY (article_id) REFERENCES article (id),
        PRIMARY KEY (date, article_id),
        UNIQUE (date, article_id, rank)
        );
        """)

        # Create indexes
        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pageview_date ON pageview (date);
        """)
        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pageview_article_id ON pageview (article_id);
        """)
        
        self.conn.commit()
    
    def close_connection(self):
        if hasattr(self, 'conn'):
            self.conn.close()
            del self.conn
            del self.cursor

    def fetch_pageviews(self, date_str):
        url = f"{PAGEVIEWS_BASE_URL}{date_str}"
        response = requests.get(url, headers=self.headers)
        if response.ok:
            response_json = response.json()
        else:
            response_json = None
        return response_json
    
    def fetch_article_categories(self, article_title):
        params = {
            'action': 'query',
            'titles': article_title,
            'prop': 'categories',
            'format': 'json',
            'cllimit': 'max'
        }
        url = f"{WIKIPEDIA_API_BASE_URL}"
        response = requests.get(url, headers=self.headers, params=params)
        if response.ok:
            response_json = response.json()
        else:
            response_json = None
        return response_json
    
    def fetch_article_text(self, article_title):
        params = {
            'action': 'query',
            'titles': article_title,
            'prop': 'extracts',
            'explaintext': True,
            'format': 'json',
            'exsectionformat': 'plain'
        }
        url = f"{WIKIPEDIA_API_BASE_URL}"
        response = requests.get(url, headers=self.headers, params=params)
        if response.ok:
            response_json = response.json()
        else:
            response_json = None
        return response_json
    
    def parse_raw_data(self, raw_data, date_str):
        if not raw_data:
            return None
        else:
            df = pd.DataFrame(raw_data['items'][0]['articles'])
            processed_df = (
                df.loc[
                    (~df['article'].str.contains(':|'.join(RESERVED_NAMESPACES))) & 
                    (df['article'] != 'Main_Page')
                ]
                .reset_index()
                .drop(columns=['index', 'rank'])
                .reset_index(names='rank')
                .assign(rank=lambda x: x['rank'] + 1,
                        date=date_str)
                .head(100)
            )
            return processed_df
        
    def get_article_id(self, article_title):

        self.cursor.execute("""SELECT id FROM article WHERE title = ?;""", (article_title,))
        article = self.cursor.fetchone()

        if article is None:
            self.cursor.execute("""INSERT INTO article (title) VALUES (?);""", (article_title,))
            article_id = self.cursor.lastrowid
        else:
            article_id = article[0]
        self.conn.commit()
        return article_id
    
    def insert_pageviews(self, row):

        if self.mode == 'sql':

            self.cursor.execute("""INSERT OR REPLACE INTO pageview (date, article_id, views, rank) VALUES (?, ?, ?, ?);""",
                            (row['date'], row['article_id'], row['views'], row['rank']))
            self.conn.commit()

    def insert_data(self, data):

        if self.mode == 'sql':
            data_dict = data.to_dict(orient='records')

            for row in data_dict:
                
                # Get or insert article_id
                article_id = self.get_article_id(row['article'])
                row['article_id'] = article_id

                # Insert daily visits
                self.insert_pageviews(row)
        else:
            data.to_csv(self.csv_path, mode='a', header=not self.csv_path.exists(), index=False)

        
    
    
