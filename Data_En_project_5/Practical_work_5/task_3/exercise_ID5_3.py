import json
from pymongo import MongoClient
import msgpack

#Функция подключения и создания БД в MongoDB:
def connect():
    client = MongoClient() #Создаём клиент для подключения к МонгоДБ
    db = client["database-1"] #Подключаемся к базе данных "database-1" в МонгоДБ
    return db.person_data #Создаем (Подключаемся) коллекцию "person_data" из первого задания в базе данных "database-1" в МонгоДБ

#Функция считывания msgpack :
def open_json(file_msgpack):
    with open(file_msgpack, "rb") as file:
        res = msgpack.load(file)
        return res

#Загружаем данные в коллекцию(таблица с данными):
def insert_many(collection, data):
    collection.insert_many(data)

#Функция удаления из коллекции документов по предикату:
# salary < 25 000 || salary > 175000
def delete_salary(collection):
    q = {"$or": [{"salary": {"$lt": 25000}},
                 {"salary": {"$gt": 175000}}]}

    res = collection.delete_many(q)

#Увеличить возраст (age) всех документов на 1
def added_age(collection):
    res = collection.update_many({}, {"$inc":{"age": 1}})


#Поднять заработную плату на 5% для произвольно выбранных профессий
def added_salary_job(collection):
    res = collection.update_many({"job": {"$in": ["Психолог", "Медсестра"]}},
                                 {"$mul": {"salary": 1.05}})

#Поднять заработную плату на 7% для произвольно выбранных городов
def added_salary_city(collection):
    res = collection.update_many({"city": {"$in": ["Минск", "Луарка", "Льейда"]}},
                                 {"$mul": {"salary": 1.07}})

#Поднять заработную плату на 10% для выборки по сложному предикату
# (произвольный город, произвольный набор профессий,
# произвольный диапазон возраста)
def added_salary(collection):
    res = collection.update_many({"$and": [{"city": {"$in": ["Льейда", "Севилья", "Бургос"]}},
                                          {"job": {"$in": ["Врач", "Архитектор"]}},
                                           {"age": {"$gt": 30, "$lt": 40}}]},
                                 {"$mul": {"salary": 1.07}})

#Удалить из коллекции записи по произвольному предикату city = "Луарка"
def delete_city(collection):
    q = {"city": "Луарка"}
    res = collection.delete_many(q)

data = open_json("task_3_item.msgpack")
#insert_many(connect(), data)
#delete_salary(connect())
#added_age(connect())
#added_salary(connect())
#added_salary_city(connect())
#added_salary(connect())
#delete_city(connect())