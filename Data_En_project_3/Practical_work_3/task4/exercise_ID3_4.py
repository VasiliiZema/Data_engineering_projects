import re
from zipfile import ZipFile
from bs4 import BeautifulSoup
import pandas as pd
import json
import lxml

#Производим разархивацию файла
archive = 'zip_var_19_4.zip'
z = ZipFile(archive)
list_file_xml = [text_file.filename for text_file in z.infolist()] #Список названий разархивированных файлов

with ZipFile(archive, 'r') as zip_file:
    zip_file.extractall()

#Создаем функцию для парсинга страницы
def parser_xml(file_xml):
    items = []
    with open(file_xml, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        soup = BeautifulSoup(text, 'xml')
        clothinges = soup.find_all("clothing")

        for clothing in clothinges:
            item = dict()

            for el in clothing.contents:
                if el.name is not None:
                    item[el.name] = el.text.strip()

            items.append(item)

    return items

#Добавляем спарсенные данные в общий список
xml_list = []

for xml_file in list_file_xml:
    xml_list += parser_xml(xml_file)

# #Сортируем по "price"  и записываем в  файл
xml_list = sorted(xml_list, key=lambda x: x["price"], reverse=True)

with open("result_4.json", 'w', encoding="utf-8") as file_json:
    file_json.write(json.dumps(xml_list))

#Фильтруем по "material" == "Шелк" и записываем в новый файл
with open("result_filter_4.json", 'w', encoding="utf-8") as file_json:
    filter_items = []

    for info in xml_list:
        if info["material"] == 'Шелк':
            filter_items.append(info)

    file_json.write(json.dumps(filter_items))

#Для числового поля 'price' считаем : максимальное и минимальное значения, среднее арифметическое,
# сумму, стандартное отклонение
item_ = dict()

df = pd.DataFrame.from_dict(xml_list)


item_["max_price"] = df['price'].astype(float).max()
item_["min_price"] = df['price'].astype(float).min()
item_["mean_price"] = df['price'].astype(float).mean()
item_["sum_dprice"] = df['price'].astype(float).sum()
item_["std_dprice"] = df['price'].astype(float).std()

print("Статистические данные поля 'price':")

for key, value in item_.items():
    print(key, value)

print("____________")

#рассчитайте частоту встречаемости для текстового поля 'category'
cont_category = df['category'].value_counts()

print("Частота встречаемости для текстового поля 'category':")
print(cont_category)