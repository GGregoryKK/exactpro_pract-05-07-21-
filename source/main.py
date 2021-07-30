from glob import glob
from time import sleep
from loguru import logger
import datetime as dt

from cas import CasInteract
from csvv import PlainCsv
from alerts import Alert


class Main:
    def __init__(self, create=None, keyspace=None, qe=None, s=None):
        self.size = 0
        self.keyspace = keyspace
        self.qe = qe
        self.s = s
        self.create = create

        self.py_driver = CasInteract()
        self.session = self.py_driver.create_session()
        self.py_driver.create_keyspace(self.keyspace)

        self.alert = Alert()

        self.alert.qe = self.qe
        if self.create:
            self.status = 'processing'
            self.alert.qe.value = 0
        else:
            self.status = 'done'
            self.alert.qe.value = 100

    def counter(self):
        self.size += 1

    def get_status(self):
        return self.status

    def update_status(self, string):
        self.status = string
        self.s.value = self.get_status()

    def get_s_status(self):
        return self.s.value

    def import_data_to_cassandra(self):
        self.py_driver.drop_all_tables()
        self.update_status(2)
        date = str(dt.datetime.strptime(str(dt.datetime.now().date()), '%Y-%m-%d').strftime('%d-%m-%Y'))
        while True:
            path = glob('transactions/transactions_current-' + date + '*.csv')
            if path:
                self.py_driver.create_transaction_table()
                pc = PlainCsv()
                pc.read(path[0])
                for row in pc.data[1:]:
                    row[6] = row[6].upper()
                    self.counter()
                self.py_driver.insert_to_transaction(pc.data[1:])
                del pc
                break
            else:
                logger.info("no such 'transactions' file")
                logger.info("re-search after 60 seconds")
                sleep(60)
        while True:
            path = glob('prices/price_file_' + date + '*.csv')
            if path:
                input_timestamp = int(path[0][29:len(path) - 5])
                offset = dt.timezone(dt.timedelta(hours=3))
                time = dt.datetime.now(offset)
                if abs(time.timestamp() - input_timestamp) < 43200:
                    self.py_driver.create_price_table()
                    pc = PlainCsv()
                    pc.read(path[0])
                    for row in pc.data[1:]:
                        row[2] = row[2].upper()
                    self.py_driver.insert_to_prices(pc.data[1:])
                    del pc
                    break
                else:
                    logger.info("no such 'prices' file")
                    logger.info("re-search after 60 seconds")
                    sleep(60)
            else:
                logger.info("no such 'prices' file")
                logger.info("re-search after 60 seconds")
                sleep(60)
        self.update_status(1)

    def run(self, qe, stat):
        self.py_driver = CasInteract()
        self.session = self.py_driver.create_session()
        self.create = self.py_driver.tables_exist() or self.create
        if self.create:
            self.s = stat
            self.update_status(1)
            self.py_driver.drop_keyspace(self.keyspace)
            self.py_driver.create_keyspace(self.keyspace)
            self.alert = Alert(self.py_driver)
            self.alert.qe = qe
            self.import_data_to_cassandra()
            self.alert.size = self.size
            self.alert.alert_collector()
            self.update_status(3)
        else:
            self.py_driver.create_keyspace(self.keyspace)
            self.alert = Alert(self.py_driver)
            self.alert.qe = qe
            self.alert.qe.value = 100
            self.update_status(3)
