import csv
from dataclasses import dataclass
from typing import List


@dataclass
class PlainCsv:
    separator: str = ','
    quote: str = '"'

    def write(self, data: List[List[str]], path):
        with open(path, 'w', newline='') as file:
            csv_writer = csv.writer(file, delimiter=self.separator, quotechar=self.quote)
            csv_writer.writerows(data)

    def read(self, path):
        with open(path, 'r', newline='') as file:
            csv_reader = csv.reader(file)
            return [row for row in csv_reader]
