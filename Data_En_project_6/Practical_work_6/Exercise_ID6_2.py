import json
import pandas as pd
import matplotlib
import numpy as np
import os

pd.set_option("display.max_rows", 20, "display.max_columns", 60)


file_name_csv = "flights.csv"
#Считываем данные
df_original = pd.read_csv(file_name_csv)

#Обработка исходного(оригинального) датасета
def memory_stat_dataset(df, file_name_csv,  write_file_json):
    print()
    print(f"Идёт обработка датасета - {file_name_csv}")
    #Размер файла в ОС
    file_size = os.path.getsize(file_name_csv)
    #Сколько занимает памяти каждая колонка отдельно
    memory_columns = df.memory_usage(deep=True)
    #сколько занимает памяти все колонки вместе)
    total_memory_columns = memory_columns.sum()
    #Сравниваем объем файла с данными в ОС и Считанный pandas в память:
    print(f"file size           = {file_size// 1024:10} КБ")
    print(f"file in memory size = {total_memory_columns// 1024:10} КБ")

    #Сколько занимает памяти каждая колонка отдельно
    memory_columns = df.memory_usage(deep=True)
    #Cколько занимает памяти все колонки вместе)
    total_memory_columns = memory_columns.sum()

    #Вычисляем для исходного датасета для каждой колонки занимаемый объем памяти, долю от общего объема, а также выяснить тип данных
    columns_info = list()
    for key in df.dtypes.keys():
        columns_info.append(
            {
                "columns_name": key,
                "memory_abs": int(memory_columns[key] // 1024),
                "memory_per": round(memory_columns[key] / total_memory_columns * 100, 4),
                "dtype": str(df.dtypes[key])
            }
        )

    #Сортируем по полю "memory_abs" - данные по занимаемому объему памяти
    columns_info.sort(key=lambda x: x["memory_abs"], reverse=True)

    print("========================================================")
    print("Колонка - обём колонки - доля обёма колонки от всего обёма датасета - тип данных колонки")
    for column in columns_info:
       print(f"{column['columns_name']:30}: {column['memory_abs']:10} КБ: {column['memory_per']:10} %: {column['dtype']:10}")

    #Записываем полученные данные в json файл
    with open(write_file_json, "w", encoding="utf-8") as file:
        file.write(json.dumps(columns_info))

    #Сколько памяти занимает каждый тип данных отдельно
    print("========================================================")
    print("Сколько памяти занимает каждый тип данных отдельно")
    for dtype in ["float", "int", "object", "category"]:
        selected_dtype = df.select_dtypes(include=[dtype])
        mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
        mean_usage_mb = mean_usage_b / 1024**2
        print("Использование памяти в среднем для {} столбцов: {:03.4f} МB".format(dtype, mean_usage_mb))

    return


#Функция определяющая сколько памяти занимают данные
def mem_usage(dataset: object) -> object:
    return round(dataset.memory_usage(deep=True).sum() / 1024**2, 3)

#Преобразовываем колонки с переменными типа object в категориальные переменные,
#если количество уникальных значений колонки составляет менее 50%
def opt_obj(df):
    df_object = df_original.select_dtypes(include=['object']).copy()
    converted_object = pd.DataFrame(columns=df_object.columns)
    for colum in df_object:
        if len(df_object[colum].unique()) / len(df_object[colum]) < 0.5:
            converted_object[colum] = df_object[colum].astype("category")
        else:
            converted_object[colum] = df_object[colum]
    #Занимаемый объем памяти до и после конвертации данных типа object
    print("========================================================")
    print("Занимаемый объем памяти до и после конвертации данных типа object")
    print("Память данных типа object    - {:.2f} MB".format(mem_usage(df_object)))
    print("Память конвертируемых данных - {:.2f} MB".format(mem_usage(converted_object)))

    return converted_object

#Проводим понижающее преобразование типов «int» колонок
def opt_int(df):
    df_int = df_original.select_dtypes(include=["int"]).copy()
    converted_int = df_int.apply(pd.to_numeric, downcast="unsigned")
    #Занимаемый объем памяти до и после конвертации данных типа int
    print("========================================================")
    print("Занимаемый объем памяти до и после конвертации данных типа int")
    print("Память данных типа int   - {:.2f} MB".format(mem_usage(df_int)))
    print("Память конвертируемых данных - {:.2f} MB".format(mem_usage(converted_int)))
    return converted_int

#Проводим понижающее преобразование типов «float» колонок
def opt_float(df):
    df_float = df_original.select_dtypes(include=["float"]).copy()
    converted_float = df_float.apply(pd.to_numeric, downcast="float")
    #Занимаемый объем памяти до и после конвертации данных типа float
    print("========================================================")
    print("Занимаемый объем памяти до и после конвертации данных типа float")
    print("Память данных типа float   - {:.2f} MB".format(mem_usage(df_float)))
    print("Память конвертируемых данных - {:.2f} MB".format(mem_usage(converted_float)))

    return converted_float

#Создаем копию исходного датасета
df_optimized = df_original.copy()
#Составляем оптимизированный датасет из конвертируемых данных
df_obj = opt_obj(df_original)
df_int = opt_int(df_original)
df_float = opt_float(df_original)
df_optimized[df_obj.columns] = df_obj
df_optimized[df_int.columns] = df_int
df_optimized[df_float.columns] = df_float

#Записываем оптимизированный датасет в csv файл
df_optimized.to_csv("flights_optimizired.csv")

#!!!!!!!
memory_stat_dataset(df_original, file_name_csv,  "memory_info_data_orijinal.json")
#!!!!!!!
memory_stat_dataset(df_optimized, "flights_optimizired.csv",  "memory_info_data_optimizired.json")

#Занимаемый объем памяти исходного и оптимизированного датасета
print("========================================================")
print("Занимаемый объем памяти исходного и оптимизированного датасета")
print("Объем занимаемой памяти исходного датасета         - {:.2f} MB".format(mem_usage(df_original)))
print("Объем занимаемой памяти оптимизированного датасета - {:.2f} MB".format(mem_usage(df_optimized)))


#Выбераем произвольно 10 колонок для дальнейшем работы

opt_dtypes = df_optimized.dtypes
need_colum = dict()

columns_name = ["DEPARTURE_TIME", "TAIL_NUMBER", "YEAR", "MONTH", "DAY", "FLIGHT_NUMBER",
             "AIRLINE", "ORIGIN_AIRPORT", "ARRIVAL_TIME", "DISTANCE"]
for key in columns_name:
    need_colum[key] = opt_dtypes[key]

#Формируем новый датасет с выбранными колонками, преобразованными к нужному типу
read_and_optimized = pd.read_csv(file_name_csv,
                                 usecols=lambda x: x in columns_name,
                                 dtype=need_colum,
                                 parse_dates=['DEPARTURE_TIME'])

#read_and_optimized = read_and_optimized[columns_name]
print("Датасет из 10 выбранных колонок, преобразованных к нужному типу")
print("Размерность датасета:  ", read_and_optimized.shape)
print("Объем занимаемой памяти оптимизированного датасета для выбранных 10 колонок:  ", mem_usage(read_and_optimized), "MB")

# Выше написанный код преобразований, можно заменить чанкой
# Формируем чанкb(какой объем датасета(строк) будем держаться в памяти)
for chunk in pd.read_csv(file_name_csv,
                        usecols=lambda x: x in columns_name,
                        dtype=need_colum,
                        parse_dates=['DEPARTURE_TIME'],
                        infer_datetime_format=True,
                        chunksize=1_000_000):
    print(f"chunks memory:   {mem_usage(chunk)} MB")
    chunk.to_csv("df_2.csv", mode="a")