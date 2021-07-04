from cassandra.cluster import Cluster, BatchStatement
from loguru import logger

class CasInteract:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None

    def __del__(self):
        self.cluster.shutdown()

    def create_session(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def drop_keyspace(self, keyspace):
        logger.info(" dropping keyspace -- %s" % keyspace)
        self.session.execute("DROP KEYSPACE " + keyspace)

    def create_keyspace(self, keyspace):
        logger.info(" creating keyspace -- %s" % keyspace)
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            pass
        else:
            self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % keyspace)
        self.session.set_keyspace(keyspace)

    def drop_table(self, table_name):
        logger.info("dropping %s table" % table_name)
        c_sql = """DROP TABLE IF EXISTS %s """ % table_name
        self.session.execute(c_sql)

    def create_price_table(self):
        logger.info(" processing")
        c_sql = """
                CREATE TABLE IF NOT EXISTS prices (Instrument_Name varchar PRIMARY KEY,
                                              Date date,
                                              Currency varchar,
                                              avgPrices float,
                                              Net_Amount_per_day float);
                 """
        self.session.execute(c_sql)

    def create_transaction_table(self):
        logger.info(" processing")
        c_sql = """
                CREATE TABLE IF NOT EXISTS transaction (Transaction_ID bigint PRIMARY KEY,
                                              Execution_Entity_Name varchar,
                                              Instrument_Name varchar,
                                              Instrument_Classification varchar,
                                              Quantity int,
                                              Price float,
                                              Currency varchar,
                                              Datestamp timestamp,
                                              Net_Amount float);
                 """
        self.session.execute(c_sql)

    def insert_to_transaction(self, data):
        logger.info(" processing")
        insert_cql = self.session.prepare("INSERT INTO  transaction (Transaction_ID, Execution_Entity_Name,  "
                                          "Instrument_Name, Instrument_Classification, Quantity, Price, Currency, "
                                          "Datestamp, Net_Amount) VALUES (?,?,?,?,?,?,?,?,?)")
        batch = BatchStatement()
        for row in data:
            batch.add(insert_cql, row)
        self.session.execute(batch)

    def insert_to_prices(self, data):
        logger.info(" processing")
        insert_cql = self.session.prepare("INSERT INTO  prices (Instrument_Name, Date , Currency, avgPrices, "
                                          "Net_Amount_per_day) VALUES (?,?,?,?,?)")
        batch = BatchStatement()
        for row in data:
            batch.add(insert_cql, row)
        self.session.execute(batch)
