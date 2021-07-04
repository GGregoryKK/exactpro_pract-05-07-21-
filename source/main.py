from loguru import logger
import datetime
from cas import CasInteract
from csvv import PlainCsv


def pepega(data):
    counter = 0
    data = data[1:]
    for row in data:
        counter +=1
        row[0] = int(row[0])
        row[4] = int(row[4])
        row[5] = float(row[5])
        row[7] = datetime.datetime.strptime(row[7], '%d-%m-%Y %H:%M:%S')
        row[8] = float(row[8])
    return data

def pepega1(data):
    counter = 0
    data = data[1:]
    for row in data:
        counter += 1
        row[1] = datetime.datetime.strptime(row[1], '%d-%m-%Y').date()
        row[3] = float(row[3])
        row[4] = float(row[4])
    return data


if __name__ == '__main__':
    ex = CasInteract()
    ex.create_session()
    ex.create_keyspace('mykspc')
    ex.drop_table("prices")
    ex.create_price_table()
    path = "prices/price_file__datestamp_.csv"
    pc = PlainCsv()
    data = pc.read(path)
    ex.insert_to_prices(pepega1(data))
    ex.drop_table("transaction")
    ex.create_transaction_table()
    path = "transactions/transactions_current__datetime_.csv"
    data = pc.read(path)
    ex.insert_to_transaction(pepega(data))
    ex.drop_keyspace("mykspc")
