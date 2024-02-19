import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import requests
from bs4 import BeautifulSoup
import time
#
import logging
import logging.handlers
import os


class StockData:
    def __init__(self, document_id, sn, symbol, ltp, point_change, cent_change, open_val, high, low, volume, prev_close):
        self.document_id = document_id
        self.sn = sn
        self.symbol = symbol
        self.ltp = ltp
        self.point_change = point_change
        self.cent_change = cent_change
        self.open = open_val
        self.high = high
        self.low = low
        self.volume = volume
        self.prev_close = prev_close

    def to_dict(self):
        return {
            'document_id': self.document_id,
            'SN': self.sn,
            'Symbol': self.symbol,
            'LTP': self.ltp,
            'Point_Change': self.point_change,
            'Cent_Change': self.cent_change,
            'Open': self.open,
            'High': self.high,
            'Low': self.low,
            'Volume': self.volume,
            'Previous_Close': self.prev_close
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            document_id=data['document_id'],
            sn=data['SN'],
            symbol=data['Symbol'],
            ltp=data['LTP'],
            point_change=data['Point_Change'],
            cent_change=data['Cent_Change'],
            open_val=data['Open'],
            high=data['High'],
            low=data['Low'],
            volume=data['Volume'],
            prev_close=data['Previous_Close']
        )

class WebScraper:
    def __init__(self, url):
        self.url = url
        self.firestore_db = None
        self.scraped_data = None
        self.setup_firestore()
        self.session = requests.Session()

    def setup_firestore(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate("nepsesharetrade-4c7f2-firebase-adminsdk-209vs-12c596495c.json")
            firebase_admin.initialize_app(cred)
            print('Initialized Firebase')
            self.firestore_db = firestore.client()
        else:
            self.firestore_db = firestore.client()

    def make_request(self):
        try:
            response = self.session.get(self.url, proxies={})
            if response.status_code == 200:
                current_time = datetime.now()
                logger.info(f'time: {current_time}')
                return response.content
            else:
                print(f"Request failed with status code: {response.status_code}")
                return None
        except requests.exceptions.ProxyError as pe:
            print(f"Proxy error occurred: {pe}")
            print("Retrying the request without using the proxy...")
            try:
                # Retry the request without using the proxy
                response = self.session.get(self.url)
                if response.status_code == 200:
                    return response.content
                else:
                    print(f"Request failed even without using the proxy. Status code: {response.status_code}")
                    return None
            except Exception as e:
                print(f"An error occurred while retrying the request without using the proxy: {e}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def scrape_table(self, html_content):
        if not html_content:
            print("No HTML content to parse")
            return None
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        if 'merolagani' in self.url:
            print('From Merolagani')
            table_object = soup.find("table", attrs={"data-live-label": "#live-trading-label-1"})  # merolagani
        else:
            print('From Sharesansar')
            table_object = soup.find("table", attrs={'id': "headFixed"})  # sharesansar
        
        if table_object is None:
            print("Table element not found.")
            return None
        
        mylist = []
        for row in table_object.find_all("tr"):
            datas = [c.text.strip() for c in row.find_all("td") if c]
            # Check if the row has at least 10 columns (adjust this number based on your data)
            if len(datas) >= 10:
                mylist.append(datas)
            else:
                print("Row doesn't have enough columns:", datas)
        
        # Now clean the data and create cleaned_data list [ Main point to change the data]
        cleaned_data = []
        for row in mylist:
            try:
                sn = float(row[0].replace(',', ''))
                symbol = row[1]
                ltp = float(row[2].replace(',', ''))
                point_change = float(row[3].replace(',', ''))
                cent_change = float(row[4].replace(',', ''))
                open_val = float(row[5].replace(',', ''))
                high = float(row[6].replace(',', ''))
                low = float(row[7].replace(',', ''))
                volume = float(row[8].replace(',', ''))
                prev_close = float(row[9].replace(',', ''))
                
                # Append the cleaned data as a tuple to the cleaned_data list
                cleaned_data.append((sn, symbol, ltp, point_change, cent_change, open_val, high, low, volume, prev_close))
            except ValueError as e:
                print(f"Invalid value in row: {row}. Error: {e}")
                continue
        
        print(cleaned_data)
        print(len(cleaned_data))
        self.scraped_data = cleaned_data # give the whole class access to this scrapped data
        return cleaned_data

    def add_all_data_to_firestore(self, data):
        print('adding all data to Firestore')
        
        if not data:
            print("No data to add to Firestore")
            return
        count = 100001 # starting the counting with 1 lakh
        for row in data:
            if len(row) >= 2:
                print('doc_'+ str(count) +'     '+ row[1])
                doc_id = 'doc_'+ str(count)
                doc_ref = self.firestore_db.collection('stocks_data').document(document_id=doc_id)

                try:
                    sn = row[0]
                    symbol = row[1]
                    ltp = row[2]
                    point_change = row[3]
                    cent_change = row[4]
                    open_val = row[5]
                    high = row[6]
                    low = row[7]
                    volume = row[8]
                    prev_close = row[9]
                except ValueError as e:
                    print(f"Invalid value in row: {row}. Error: {e}")
                    continue

                stock_data = StockData(
                    document_id=doc_id,
                    sn=sn,
                    symbol=symbol,
                    ltp=ltp,
                    point_change=point_change,
                    cent_change=cent_change,
                    open_val=open_val,
                    high=high,
                    low=low,
                    volume=volume,
                    prev_close=prev_close
                )

                doc_ref.set(stock_data.to_dict())
                count += 1
            else:
                print("Row doesn't have enough elements:", row)

    def data_changed(self, new_data):
        # Get 30 random (sn, Symbol, LTP) tuples from Firestore
        random_ltp_firestore = self.get_random_ltp_from_firestore(30)

        # Extract 30 random (sn, Symbol, LTP) tuples from the new data
        scrapped_db = [(row[0], row[1], row[2]) for row in new_data if len(row) >= 2]
        print('random_ltp_firestore')
        print(random_ltp_firestore)
        print('scrapped_db')
        print(scrapped_db)

        # Iterate through each symbol in random_ltp_firestore
        for sn_firestore, symbol_firestore, ltp_firestore in random_ltp_firestore:
            # Find the corresponding LTP in random_ltp_new_data
            for sn_scrapped, symbol_scrapped, ltp_scrapped in scrapped_db:
                if symbol_scrapped == symbol_firestore:
                    print(f'Checking if ltp same of scrapped and firestore db of {symbol_firestore}')
                    print(symbol_scrapped)
                    print(ltp_firestore)
                    print(symbol_firestore)
                    print(ltp_scrapped)
                    if ltp_firestore == ltp_scrapped:
                        print('same')
                        break
                    else:
                        print('different')
                        return True  # data has changed
        return False # data not changed

    def get_random_ltp_from_firestore(self, count):
        snapshot = self.firestore_db.collection('stocks_data').limit(count).stream()
        ltp_values = []
        for doc in snapshot:
            data = doc.to_dict()
            ltp_values.append((data.get('SN'),data.get('Symbol'), data.get('LTP')))
        return ltp_values

def main():
    url = "https://www.sharesansar.com/live-trading"
    # url = "https://merolagani.com/LatestMarket.aspx"
    scraper = WebScraper(url)
    html_content = scraper.make_request()
    if html_content:
        data = scraper.scrape_table(html_content)
        if data:
            print([c[1:3] for c in data if c])
            print(data)
            if scraper.data_changed(data):
                scraper.add_all_data_to_firestore(data)
            else:
                print('Data has not changed. No need to add to Firestore.')
    print('Job Done')




logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    #logger.info("Token not available!")
    #raise




if __name__ == "__main__":
    start_time = time.time()

# Your Python script code here
# For example:
# python main.py
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution time:", execution_time, "seconds")
    
