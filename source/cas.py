from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import named_tuple_factory, dict_factory
from loguru import logger


class CasInteract:
    def __init__(self, keyspace=None):
        self.cluster = None
        self.session = None
        self.keyspace = keyspace

    def __del__(self):
        self.cluster.shutdown()

    def create_session(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def drop_keyspace(self, keyspace):
        logger.info(" dropping keyspace -- %s" % keyspace)
        self.session.execute("DROP KEYSPACE IF EXISTS %s" % keyspace)

    def keyspace_exist(self, keyspace):
        self.keyspace = keyspace
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            return True
        else:
            return False

    def tables_exist(self):
        rows = self.session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'mykspc'")
        lst = [row[1] for row in rows]
        if len(lst) != 0:
            return False
        else:
            return True

    def create_keyspace(self, keyspace):
        self.keyspace = keyspace
        logger.info(" creating keyspace -- %s" % keyspace)
        self.session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS %s
                        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                        """ % keyspace.lower())
        self.session.set_keyspace(keyspace)

    def drop_all_tables(self):
        rows = self.session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '%s'" % self.keyspace)
        names = [row[1] for row in rows]
        for name in names:
            self.drop_table(name)

    def drop_table(self, table_name):
        logger.info("dropping %s table" % table_name)
        query = """DROP TABLE IF EXISTS %s """ % table_name
        self.session.execute(query)

    def create_price_table(self):
        logger.info("creating prices table")
        query = """
                CREATE TABLE IF NOT EXISTS prices (Instrument_Name varchar,
                                              Date varchar,
                                              Currency varchar,
                                              avgPrices varchar,
                                              Net_Amount_per_day varchar,
                                              PRIMARY KEY((Instrument_Name),Currency));
                 """
        self.session.execute(query)

    def create_simple_alert(self, table_name, data):
        logger.info("creating %s table" % table_name)
        query = """
                        CREATE TABLE IF NOT EXISTS %s ("alertid" varchar,
                                                      "alertType" varchar,
                                                      "description" varchar,
                                                      "affectedTransactionsCount" varchar,
                                                      PRIMARY KEY ("alertid"));
                         """ % table_name
        self.session.execute(query)
        logger.info("insert data to %s" % table_name)
        query = self.session.prepare(
           """INSERT INTO  %s (alertid, "alertType", "description", "affectedTransactionsCount") VALUES (?,?,?,?)""" % table_name)
        batch = BatchStatement()
        for row in data:
            batch.add(query, row)
        self.session.execute(batch)

    def create_alert_table(self, table_name):
        logger.info("creating %s table" % table_name)
        query = """
                CREATE TABLE IF NOT EXISTS %s ("alertid" varchar,
                                              "transactionid" varchar,
                                              "executionEntityName" varchar,
                                              "instrumentName" varchar,
                                              "instrumentClassification" varchar,
                                              "quantity" varchar,
                                              "price" varchar,
                                              "currency" varchar,
                                              "datestamp" varchar,
                                              "netAmount" varchar,
                                              PRIMARY KEY ("transactionid", "alertid"));
                 """ % table_name
        self.session.execute(query)

    def insert_to_alert_table(self, table_name, data):
        logger.info("insert data to %s" % table_name)
        query = self.session.prepare("""INSERT INTO  %s ("alertid", "currency", "datestamp","executionEntityName",  
        "instrumentClassification", "instrumentName", "netAmount", "price", "quantity", "transactionid") VALUES (?,?,
        ?,?,?,?,?,?,?,?)""" % table_name)
        batch = BatchStatement()
        for row in data:
            batch.add(query, row)
        self.session.execute(batch)

    def create_transaction_table(self):
        logger.info("creating %s table" % "transaction")
        c_sql = """
                CREATE TABLE IF NOT EXISTS transaction (Transaction_ID varchar,
                                              Execution_Entity_Name varchar,
                                              Instrument_Name varchar,
                                              Instrument_Classification varchar,
                                              Quantity varchar,
                                              Price varchar,
                                              Currency varchar,
                                              Datestamp varchar,
                                              Net_Amount varchar,
                                              PRIMARY KEY (Transaction_ID, Execution_Entity_Name));
                 """
        self.session.execute(c_sql)

    def insert_to_transaction(self, data):
        logger.info("insert data to transaction")
        insert_cql = self.session.prepare("INSERT INTO  transaction (Transaction_ID, Execution_Entity_Name,  "
                                          "Instrument_Name, Instrument_Classification, Quantity, Price, Currency, "
                                          "Datestamp, Net_Amount) VALUES (?,?,?,?,?,?,?,?,?)")
        batch = BatchStatement()
        for row in data:
            batch.add(insert_cql, row)
        self.session.execute(batch)

    def insert_to_prices(self, data):
        logger.info("insert data to prices")
        insert_cql = self.session.prepare("INSERT INTO  prices (Instrument_Name, Date , Currency, avgPrices, "
                                          "Net_Amount_per_day) VALUES (?,?,?,?,?)")
        batch = BatchStatement()
        for row in data:
            batch.add(insert_cql, row)
        self.session.execute(batch)

    def get_columns(self, *cols, table):
        lst = [[] for _ in cols]
        rows = self.session.execute('select %s from %s' % (",".join(cols), table))
        for _ in rows:
            index = 0
            for i in cols:
                lst[index].append(eval("_.%s" % i))
                index += 1
        return lst

    def get_table_name_fields(self, table):
        names = self.session.execute(
            "SELECT * FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'" % (
                self.keyspace, table))
        return [name.column_name for name in names]

    def get_rows_by_transaction_ids(self, ids):
        query = "select * from transaction WHERE transaction_id = '%s'"
        names = [self.get_table_name_fields("transaction")]
        lst = []
        row = self.session.execute(query % ids)
        for _ in row:
            for col in names[0]:
                lst.append(eval("_.%s" % col))
        return lst

    def get_price_avg(self, inn, cur):
        query = "select avgprices from prices WHERE instrument_name = '%s' AND currency = '%s'"
        napd = self.session.execute(query % (inn, cur))
        if napd:
            for _ in napd:
                return eval("_.%s" % "avgprices")
        else:
            return -1

    def get_data(self, table_name):

        self.session.row_factory = dict_factory
        query = ("SELECT * from %s " % table_name)
        rows = self.session.execute(query)
        self.session.row_factory = named_tuple_factory
        return [row for row in rows]
