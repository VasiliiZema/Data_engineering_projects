import re
from zipfile import ZipFile
from bs4 import BeautifulSoup
import pandas as pd
import json
import lxml

#Производим разархивацию файла
archive = 'zip_var_19_3.zip'
z = ZipFile(archive)
list_file_xml = [text_file.filename for text_file in z.infolist()] #Список названий разархивированных файлов

with ZipFile(archive, 'r') as zip_file:
    zip_file.extractall()

#Создаем функцию для парсинга страницы
def parser_xml(file_xml):
    with open(file_xml, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        soup = BeautifulSoup(text, 'xml')

        item = dict()

        item['name'] = soup.find_all("name")[0].text.strip()
        item['constellation'] = soup.find_all("constellation")[0].text.strip()
        item['spectral-class'] = soup.find_all("spectral-class")[0].text.strip()
        item['radius'] = soup.find_all("radius")[0].text.strip()
        item['rotation'] = soup.find_all("rotation")[0].text.strip()
        item['age'] = soup.find_all("age")[0].text.strip()
        item['distance'] = soup.find_all("distance")[0].text.strip()
        item['absolute-magnitude'] = soup.find_all("absolute-magnitude")[0].text.strip()

    return item

#Добавляем спарсенные данные в общий список
xml_list = []

for xml_file in list_file_xml:
    xml_list.append(parser_xml(xml_file))

#Сортируем по "distance"  и записываем в  файл
xml_list = sorted(xml_list, key=lambda x: x["distance"], reverse=True)

with open("result_3.json", 'w', encoding="utf-8") as file_json:
    file_json.write(json.dumps(xml_list))

#Фильтруем по "constellation" == "Дева" и записываем в новый файл
with open("result_filter_3.json", 'w', encoding="utf-8") as file_json:
    filter_items = []

    for info in xml_list:
        if info["constellation"] == 'Дева':
            filter_items.append(info)

    file_json.write(json.dumps(filter_items))

#Для числового поля 'distance' считаем : максимальное и минимальное значения, среднее арифметическое,
# сумму, стандартное отклонение
item_ = dict()

df = pd.DataFrame.from_dict(xml_list)

df["distance"] = df["distance"].str.replace("million km", "").astype(float)

item_["max_distance"] = f"{df['distance'].max()} million km"
item_["min_distance"] = f"{df['distance'].min()} million km"
item_["mean_distance"] = f"{df['distance'].mean()} million km"
item_["sum_distance"] = f"{df['distance'].sum()} million km"
item_["std_distance"] = f"{df['distance'].std()} million km"

print("Статистические данные поля 'distance':")

for key, value in item_.items():
    print(key, value)

print("____________")

#рассчитайте частоту встречаемости для текстового поля 'constellation'
cont_constellation = df['constellation'].value_counts()

print("Частота встречаемости для текстового поля 'constellation:")
print(cont_constellation)