from fetcher import WikipediaFetcher
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

HEADERS = {
    "User-Agent": "CuriosityIndexBot/0.1 (https://github.com/vtasca)",
    "Authorization": "Bearer " + os.getenv("MEDIAWIKI_ACCESS_TOKEN"),
}

def main():
    fetcher = WikipediaFetcher(headers=HEADERS, mode='csv', csv_path='pageviews.csv')

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')

    raw_data = fetcher.fetch_pageviews(yesterday)

    if raw_data:
        processed_data = fetcher.parse_raw_data(raw_data, yesterday)

        if processed_data is not None:

            last_appended_date = fetcher.read_last_csv_row()[-1]

            if last_appended_date == yesterday:
                print(f"Data for {yesterday} has already been appended")
                return

            fetcher.insert_data(processed_data)
            print(f"Successfully inserted data for {yesterday}")
        else:
            print(f"No data to insert for {yesterday}")
    else:
        print(f"Failed to fetch data for {yesterday}")

    fetcher.close_connection()

if __name__ == "__main__":
    main()