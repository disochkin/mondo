import csv
import re
from pymongo import MongoClient
from prettytable import PrettyTable


def get_pretty_table(table=None):
    if len(table) > 0:
        result = PrettyTable()
        result.field_names = list(table[0].keys())
        for item in table:
            result.add_row(item.values())
    else:
        return None
    return result


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    events = db["events"]
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        events.insert_many(reader)

def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    events = db["events"]
    query_result = list(events.find({}, {"_id": 0}).sort("Price", 1))
    print(get_pretty_table(query_result))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    events = db["events"]
    regex = re.compile(f"{name}")
    query_result = list(events.find({"Исполнитель": regex}, {"_id": 0}).sort("Price", 1))
    print(get_pretty_table(query_result))


def create_db():
    client = MongoClient('mongodb://login:some_password@192.168.x.x:27017/')
    db = client['app']
    return db


if __name__ == '__main__':
    db = create_db()
    read_data("artists.csv", db)
    find_cheapest(db)
    find_by_name("Seconds to", db)
    find_by_name("zz", db)
