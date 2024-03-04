import json
from pymongo import MongoClient
import pickle

#Функция подключения и создания БД в MongoDB:
def connect():
    client = MongoClient() #Создаём клиент для подключения к МонгоДБ
    db = client["database-1"] #Подключаемся к базе данных "database-1" в МонгоДБ
    return db.person_data #Создаем (Подключаемся) коллекцию "person_data" из первого задания в базе данных "database-1" в МонгоДБ

#Функция считывания pkl :
def open_json(file_pkl):
    with open(file_pkl, "rb") as file:
        res = pickle.load(file)
        return res

#Загружаем данные в коллекцию(таблица с данными):
def insert_many(collection, data):
    collection.insert_many(data)

#Функция вывода минимальной, средней, максимальной salary
def describe_salary(collection):
    q = [
        {"$group": {"_id": "res", "max": {"$max":"$salary"},
                                  "min": {"$min":"$salary"},
                                  "avg": {"$avg":"$salary"}}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_salary.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))


#Функция вывода количества данных по представленным профессиям
def count_job(collection):
    q = [
        {"$group": {"_id": "$job", "count_job":{"$sum": 1}}},
        {"$sort": {"count_job":-1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("count_job.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимальной, средней, максимальной salary по городу
def salary_city(collection):
    q = [
        {"$group": {"_id": "$city",
                    "max_salary":{"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary":{"$avg": "$salary"}}
         },
        {"$sort": {"_id":1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("salary_city.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимальной, средней, максимальной salary по профессии
def salary_job(collection):
    q = [
        {"$group": {"_id": "$job",
                    "max_salary":{"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary":{"$avg": "$salary"}}
         },
        {"$sort": {"_id":1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("salary_job.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимального, среднего, максимального возраста по городу
def age_sity(collection):
    q = [
        {"$group": {"_id": "$city",
                    "max_age":{"$max": "$age"},
                    "min_age": {"$min": "$age"},
                    "avg_age":{"$avg": "$age"}}
         },
        {"$sort": {"_id":1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("age_sity.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимального, среднего, максимального возраста по профессии
def age_job(collection):
    q = [
        {"$group": {"_id": "$job",
                    "max_age":{"$max": "$age"},
                    "min_age": {"$min": "$age"},
                    "avg_age":{"$avg": "$age"}}
         },
        {"$sort": {"_id":1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("age_job.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода максимальной заработной платы при минимальном возрасте
def max_salary_min_age(collection):
    q = [
        {
            '$sort': {'age': 1,
                   'salary': -1}
        },
        {
            '$limit': 1
        }
        ]

    items = []
    for row in collection.aggregate(q):
        del row["_id"]
        items.append(row)

    with open("max_salary_min_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимальной заработной платы при максимальной возрасте
def min_salary_max_age(collection):
    q = [
        {
            '$sort': {'age': -1,
                   'salary': 1}
        },
        {
            '$limit': 1
        }
        ]
    items = []

    for row in collection.aggregate(q):
        del row["_id"]
        items.append(row)

    with open("min_salary_max_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимального, среднего, максимального возраста по городу,
# при условии, что заработная плата больше 50 000,
# отсортировать вывод по любому полю.
def describe_age_sity(collection):
    q = [
        {"$match": {"salary": {"$gt": 50000}}},
        {"$group": {"_id": "$city",
                    "max_age": {"$max": "$age"},
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"}}
        },
        {"$sort": {"_id": 1}}
        ]

    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_age_sity.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимальной, средней, максимальной salary
# в произвольно заданных диапазонах по 1)городу, 2)профессии, и 3)возрасту:
# 18<age<25 & 50<age<65

#1)городу
def describe_salary_sity(collection):
    q = [
        {"$match": {"$or": [{"age": {"$gt": 18, "$lte": 25}},
                {"age": {"$gt": 50, "$lt": 65}}]}},
        {"$group": {"_id": "$city",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
        },
        {"$sort": {"_id": 1}}
        ]

    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_salary_sity.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#2)профессии
def describe_salary_job(collection):
    q = [
        {"$match": {"$or": [{"age": {"$gt": 18, "$lte": 25}},
                {"age": {"$gt": 50, "$lt": 65}}]}},
        {"$group": {"_id": "$job",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
        },
        {"$sort": {"_id": 1}}
        ]

    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_salary_job.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#3)возрасту
def describe_salary_age(collection):
    q = [
        {"$match": {"$or": [{"age": {"$gt": 18, "$lte": 25}},
                {"age": {"$gt": 50, "$lt": 65}}]}},
        {"$group": {"_id": "$age",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
        },
        {"$sort": {"_id": 1}}
        ]

    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_salary_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Произвольный запрос: какие в городе Минске максимальные, минимальные, средние
#зарплаты у разных специальностей, и отсортируем по специальностям в алфавитном порядке.
def salary_job_Minsk(collection):
    q = [
        {"$match": {"city": "Минск"}},
        {"$group": {"_id": "$job",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
        },
        {"$sort": {"_id": 1}}
        ]

    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("salary_job_Minsk.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

data = open_json("task_2_item.pkl")
##insert_many(connect(), data)
#describe_salary(connect())
#count_job(connect())
#salary_city(connect())
#salary_job(connect())
#age_sity(connect())
#age_job(connect())
max_salary_min_age(connect())
min_salary_max_age(connect())
#describe_age_sity(connect())
#describe_salary_sity(connect())
#describe_salary_job(connect())
#describe_salary_age(connect())
#salary_job_Minsk(connect())
