import csv
import json

from pymongo import MongoClient
import pandas as pd

#Функция подключения и создания БД в MongoDB:
def connect():
    client = MongoClient() #Создаём клиент для подключения к МонгоДБ
    db = client["database-1"] #Создаём базу данных "database-1" в МонгоДБ
    return db.chess_tournaments #Создаём коддекцию "pchess_tournaments" в базе данных "database-1" в МонгоДБ

#Функция для парсинга данных из текстового файла
def parse_data_txt(file_text):
    items = []
    with open(file_text, "r", encoding="utf-8") as file:
        line_text = file.readlines()
        item = dict()

        for row in line_text:
            if row == "=====\n":
                items.append(item)
                item = dict()

            else:
                line = row.split("::")

                if line[0] in ("tours_count", "min_rating", "time_on_game"):
                    item[line[0]] = int(line[1])
                elif line[0] == "id":
                    continue
                else:
                    item[line[0]] = line[1].strip()

    return items

#Функция для парсинга данных из csv файла
def parse_data_csv(file_csv):
    items = []
    with open(file_csv, "r", encoding="utf-8") as file:
        res = file.readlines()

    items = []

    for row in res[2:]:
        item = dict()
        if row == '\n':
            continue
        line = row.split(";")
        item['id'] = int(line[0].strip())
        item['name'] = line[1].strip()
        item['city'] = line[2].strip()
        item['begin'] = line[3].strip()
        item['system'] = line[4].strip()
        item['tours_count'] = int(line[5].strip())
        item['min_rating'] = int(line[6].strip())
        item['time_on_game'] = int(line[7].strip())

        items.append(item)

    return items

#Загружаем данные в коллекцию(таблица с данными):
def insert_many(collection, data):
    collection.insert_many(data)

#Функция вывода первых 10 записей, отсортированных по убыванию по полю time_on_game:
def sort_time_on_game(collection):

    items = []
    for game in collection.find({}, limit=10).sort({"time_on_game": -1}):
        game["_id"] = str(game["_id"])
        items.append(game)

    with open("sort_time_on_game.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода первых 15 записей, отфильтрованных по предикату tours_count < 5, отсортировать по убыванию по полю tours_count:
def filter_tours_count(collection):
    items = []
    for game in collection.find({"tours_count": {"$lt": 10}}, limit=15).sort({"tours_count": -1}):
        game["_id"] = str(game["_id"])
        print(game)
        items.append(game)

    with open("filter_tours_count.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывод первых 10 записей, отфильтрованных по сложному предикату:
# (записи только из произвольного города, записи только из трех произвольно
# взятых систем турниров system), отсортировать по возрастанию по общему времени игры time_on_game
def filter_city_system(collection):
    items = []
    for game in (collection.find({"city": "Киев", "system":{"$in" : ["circular", "Olympic", "Swiss"]}}, limit=10)
            .sort({"time_on_game": 1})):
        game["_id"] = str(game["_id"])
        items.append(game)

    with open("filter_city_system.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода количества записей, получаемых в результате следующей фильтрации
# 2400 <  min_rating <= 2450 || 2500 min_rating < 2550).
def filter_min_raiting(collection):
    items = []
    for game in (collection.find({
                "$or": [{"min_rating": {"$gt": 2400, "$lte": 2450}},
                {"min_rating": {"$gt": 2500, "$lt": 2550}}]
                                    })):
        game["_id"] = str(game["_id"])
        items.append(game)

    with open("filter_min_raiting.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимальной, средней, максимальной продолжительности игры time_on_game
def describe_time_on_game(collection):
    q = [
        {"$group": {"_id": "res", "max": {"$max":"$time_on_game"},
                                  "min": {"$min":"$time_on_game"},
                                  "avg": {"$avg":"$time_on_game"}}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_time_on_game.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода количества данных по представленным турнирам
def count_tournament(collection):
    q = [
        {"$group": {"_id": "$job", "count_name":{"$sum": 1}}},
        {"$sort": {"count_name":-1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("count_tournament.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимального, среднего, максимального рейтинга min_rating по городу
def rating_tournament(collection):
    q = [
        {"$group": {"_id": "$city",
                    "max_rating":{"$max": "$min_rating"},
                    "min_rating": {"$min": "$min_rating"},
                    "avg_rating":{"$avg": "$min_rating"}}
         },
        {"$sort": {"_id":1}}
    ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("rating_tournament.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода максимальной количества туров tours_count, минимальной продолжительности игры time_on_game
# в определенной системе проведения турнира system
def max_tours_min_time(collection):
    q = [
        {"$group": {"_id": "$system",
                    "min_time_on_game": {"$min": "$time_on_game"},
                    "max_tours_count": {"$max": "$tours_count"}}
        }
        ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("max_tours_min_time.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция вывода минимального, среднего, максимального рейтинга по городу,
# при условии, что симстема турнира Swiss,
# отсортировать вывод по любому полю.
def describe_rating_city(collection):
    q = [
        {"$match": {"system": "Swiss"}},
        {"$group": {"_id": "$city",
                    "max_rating": {"$max": "$min_rating"},
                    "min_rating": {"$min": "$min_rating"},
                    "avg_rating": {"$avg": "$min_rating"}}
        },
        {"$sort": {"_id": 1}}
        ]
    items = []
    for row in collection.aggregate(q):
        items.append(row)

    with open("describe_rating_city.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False))

#Функция удаления из коллекции документов по предикату:
# min_rating < 2450
def delete_min_rating(collection):
    q = {"min_rating": {"$gt": 2450}}

    res = collection.delete_many(q)

#Увеличить рейтинг (min_rating) всех игроков на 150
def added_arating(collection):
    res = collection.update_many({}, {"$inc":{"min_rating": 150}})

#Удалить из коллекции записи по произвольному предикату city = "Картахена"
def delete_city(collection):
    q = {"city": "Картахена"}
    res = collection.delete_many(q)

#Уменьшить время игры на 15% для произвольно выбранных турниров
def decrease_time_tournament(collection):
    res = collection.update_many({"name": {"$in": ["Аэрофлот Опен 1989", "Европа 1992"]}},
                                 {"$mul": {"time_on_game": 0.85}})

#Увеличить рейтинг (min_rating) игроков по сложному предикату на 10%
# (произвольный город, произвольная система проведения турнира)
def added_rating(collection):
    res = collection.update_many({"$and": [{"city": {"$in": ["Овьедо", "Камбадос", "Вильнюс"]}},
                                          {"system": {"$in": ["Swiss", "Olympic"]}}]},
                                 {"$mul": {"min_rating": 1.1}})

data = parse_data_txt("chess_tournaments_1.text") + parse_data_csv("chess_tournaments_2.csv")
#insert_many(connect(), data)
#sort_time_on_game(connect())
#filter_tours_count(connect())
#filter_city_system(connect())
#filter_min_raiting(connect())
#describe_time_on_game(connect())
#count_tournament(connect())
#rating_tournament(connect())
#max_tours_min_time(connect())
#describe_rating_city(connect())
#delete_min_rating(connect())
#added_arating(connect())
#delete_city(connect())
#decrease_time_tournament(connect())
#added_rating(connect())

