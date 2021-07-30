import os
from datetime import datetime
from loguru import logger

from csvv import PlainCsv


class Alert:
    def __init__(self, pydr=None):
        self.ica_transact_id = []
        self.ppa_transact_id = []
        self.ica_data = []
        self.count = 0
        self.date = str(datetime.strptime(str(datetime.now().date()), '%Y-%m-%d').strftime('%d%m%Y'))
        self.ppa_data = []
        self.pydr = pydr
        self.table_name = "alert" + self.date
        self.size = 1
        self.progress = 0
        self.affected = 0
        self.qe = None

    def get_qe(self):
        return self.qe.value

    def counter(self):
        self.count += 1
        return self.count

    def progress_counter(self):
        self.progress += 1
        self.qe.value = self.get_progress()

    def get_progress(self):
        this = round((self.progress/self.size)*100, 2)
        if this > 100:
            return 100
        if self.progress == 0:
            return 100
        return this

    @classmethod
    def sets(cls, one, two, three):
        sorted_exen, exen, inn = set(one), two, three
        sorted_inn = []
        for een in sorted_exen:
            sublist = []
            for i in range(len(inn)):
                if exen[i] == een:
                    sublist.append(inn[i])
            sorted_inn.append(list(set(sublist)))
        return list(zip(sorted_exen, sorted_inn))

    def ica(self):
        logger.info("search for ICA alerts")
        columns = self.pydr.get_columns("execution_entity_name", "instrument_name", "currency", "transaction_id", table='transaction')
        zp = self.sets(columns[0], columns[0], columns[1])
        msg = "Currency field is incorrect for the combination of {} and {}"
        for i in zp:
            for j in i[1]:
                s = 0
                id_list = []
                for k in range(len(columns[2])):
                    cur = columns[2][k]
                    if j == columns[1][k] and i[0] == columns[0][k]:
                        self.progress_counter()
                        if not (PlainCsv.is_latin(cur) and len(cur) == 3):
                            id_list.append(columns[3][k])
                            s += 1
                if s != 0:
                    num = str(self.counter())
                    self.ica_transact_id.append([(i, "ICA" + self.date + num) for i in id_list])
                    self.ica_data.append(["ICA" + self.date + num, "ICA", msg.format(i[0], j), str(len(id_list))])
        self.affected = self.count

    def ppa(self):
        logger.info("search for PPA alerts")
        columns = self.pydr.get_columns("execution_entity_name", "instrument_name", "currency","price", "transaction_id", table='transaction')
        zp = self.sets(columns[0], columns[0], columns[1])
        list_curr = list(set(i for i in columns[2] if PlainCsv.is_latin(i) and len(i) == 3))
        msg = "Potential pumping price activity has been noticed for the following combination of {} and {} and {} " \
              "where an average price is greater than previous more than 50% and is {} "
        self.progress += self.affected
        for i in zp:
            for lc in list_curr:
                for j in i[1]:
                    s = 0
                    id_list = []
                    for k in range(len(columns[2])):
                        if j == columns[1][k] and i[0] == columns[0][k] and columns[2][k] == lc:
                            self.progress_counter()
                            s += float(columns[3][k])
                            id_list.append(columns[4][k])
                    if s != 0:
                        explicit_avg = round(s / len(id_list), 2)
                        av_prices = float(self.pydr.get_price_avg(j, lc))
                        more_than = round((explicit_avg/av_prices - 1)*100, 2)
                        if more_than > 50:
                            num = str(self.counter())
                            self.ppa_data.append(["PPA" + self.date + num, "PPA", msg.format(i[0], j, lc, more_than), str(len(id_list))])
                            sublist = [(i, "PPA" + self.date + num) for i in id_list]
                            self.ppa_transact_id.append(sublist)

                            del sublist

    def create_alerts_tables(self):
        lst = [self.ica_transact_id, self.ppa_transact_id]
        for data in lst:
            for i in data:
                table_name = i[0][1]
                self.pydr.create_alert_table(table_name)
                lst = []
                for j in i:
                    g = self.pydr.get_rows_by_transaction_ids(str(j[0]))
                    g.insert(0, j[1])
                    lst.append(g)
                self.pydr.insert_to_alert_table(table_name, lst)
        del self.ica_transact_id, self.ppa_transact_id

    def alert_collector(self):
        self.size *= 2
        self.ica()
        self.ppa()
        self.create_alerts_tables()
        col_names = ["Aler ID", "Aler type", "Description", "Affected transactions count"]
        alert_lst = [col_names]

        if len(self.ica_data) > 0:
            alert_lst.extend(self.ica_data)
            del self.ica_data
        if len(self.ppa_data) > 0:
            alert_lst.extend(self.ppa_data)
            del self.ppa_data

        if not os.path.isdir("output"):
            os.mkdir("output")
        path = "output/alerts_" + self.date + ".csv"
        p = PlainCsv()
        p.write(alert_lst, path)
        del p
        self.pydr.create_simple_alert(self.table_name, alert_lst[1:])
        logger.info("Alerts created")
