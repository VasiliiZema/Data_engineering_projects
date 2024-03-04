import csv
import json
import msgpack
import os.path
import pandas as pd
import pickle

df = pd.read_csv('Alzheimer_s_Disease_and_Healthy_Aging_Data.csv')
df = df[["YearStart", "LocationDesc", "Stratification1", "Stratification2", "Data_Value_Alt",
         "Low_Confidence_Limit", "High_Confidence_Limit"]]

#print(df.dtypes)

df["YearStart"] = df["YearStart"].astype(object)
df["Low_Confidence_Limit"] = df["Low_Confidence_Limit"].replace('.', 'nan').astype(float)
df["High_Confidence_Limit"] = df["High_Confidence_Limit"].replace('.', 'nan').astype(float)

num_dict = dict()
str_dict = dict()

for row in df:
    test_num_dict = dict()
    test_str_dict = dict()

    # Для полей, представляющих числовые данные, рассчитайте характеристики:
    # максимальное и минимальное значения, среднее арифметическое, сумму, стандартное отклонение
    if df[row].dtypes in (int, float):
        test_num_dict["max_value"] = df[row].max()
        test_num_dict["min_value"] = df[row].min()
        test_num_dict["avg_value"] = df[row].mean()
        test_num_dict["sum_value"] = df[row].sum()
        test_num_dict["std_value"] = df[row].std()

        num_dict[row] = test_num_dict
    else: #Для полей, представляющий текстовые данные рассчитайте частоту встречаемости.
        for elem in df[row]:
            if ("value_count " + str(elem)) in test_str_dict:
                test_str_dict["value_count " + str(elem)] += 1
            else:
                test_str_dict["value_count " + str(elem)] = 0

            str_dict[row] = test_str_dict

# Сораняем расчеты для числовых полей в json файл
with open('num_result_5.json', 'w') as file_json:
    file_json.write(json.dumps(num_dict))

# Сораняем частоту встречаемости для меток строковых полей в json файл
with open('str_result_5.json', 'w') as file_json:
    file_json.write(json.dumps(str_dict))


# Сораняем наш набор данных в  файл в msgpack файл
with open('Alzheimer_s_Disease_and_Healthy_Aging_Data.msgpack', "wb") as file_msgpack:
    file_msgpack.write(msgpack.dumps(df.to_dict()))

# Сораняем наш набор данных в  файл в pkl файл
# df.to_pickle("Alzheimer_s_Disease_and_Healthy_Aging_Data.pkl")
with open("Alzheimer_s_Disease_and_Healthy_Aging_Data.pkl", "wb") as file_pkl:
    file_pkl.write(pickle.dumps(df.to_dict()))

# Сораняем наш набор данных в  файл в json файл
# df.to_json("Alzheimer_s_Disease_and_Healthy_Aging_Data.json")
with open('Alzheimer_s_Disease_and_Healthy_Aging_Data.json', 'w') as file_json:
    file_json.write(json.dumps(df.to_dict()))

#Рассчитываем размер каждого файла
ends = ('_Data.csv', '_Data.json', '_Data.msgpack', '_Data.pkl')
for file in os.scandir():
    if file.name.endswith(ends):
        print(file.name, os.path.getsize(file))




