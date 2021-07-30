import csv
from dataclasses import dataclass
from typing import List
from string import ascii_letters


@dataclass
class PlainCsv:
    separator: str = ','
    quote: str = '"'
    data = List[List[str]]
    input_check = False

    def write(self, data: List[List[str]], path):
        with open(path, 'w', newline='') as file:
            csv_writer = csv.writer(file, delimiter=self.separator, quotechar=self.quote)
            csv_writer.writerows(data)

    def read(self, path):
        with open(path, 'r', newline='') as file:
            csv_reader = csv.reader(file)
            self.data = [row for row in csv_reader]

    @staticmethod
    def is_latin(string):
        return all(map(lambda c: c in ascii_letters, string))
