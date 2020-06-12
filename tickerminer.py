import yfinance as yf
from pymongo import MongoClient
from pandas_datareader import data as pdr

yf.pdr_override()

class TickerMiner():
    def __init__(self):
        # Инициализация клиента MongoDB
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.stockdb

    # Получение и запись подробной информации о компаниях
    def getTickerDetails(self):
        count = 0
        for document in self.db['companies'].find():
            count += 1
            print("Loading: ", count, document['symbol'])
            ticker = yf.Ticker(document['symbol'])
            try:
                data = ticker.info
                data = {k: v for k, v in data.items() if v is not None}
                self.db['companies.details'].insert_one(data)
            except KeyboardInterrupt:
                break
            except:
                print(document['symbol'], "skipped")
    
    # Получение и запись исторических данных о торгах
    def getAllStocks(self):
        count = 0
        for document in self.db['companies'].find(no_cursor_timeout=True):
            count += 1
            print("Loading: ", count, document['symbol'])
            data = pdr.get_data_yahoo(document['symbol'])
            for index, row in data.iterrows():
                trade = {
                        'symbol_id': document['symbol'],
                        'date': index,
                        'open': row['Open'],
                        'high': row['High'],
                        'low': row['Low'],
                        'close': row['Close'],
                        'adjClose': row['Adj Close'],
                        'volume': row['Volume']
                    }
                self.db['companies.stocks'].insert_one(trade)


if __name__ == '__main__':
    app = TickerMiner()
    app.getTickerDetails()
    app.getAllStocks()
