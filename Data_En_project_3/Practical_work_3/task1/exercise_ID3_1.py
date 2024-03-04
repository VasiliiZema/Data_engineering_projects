import re
from zipfile import ZipFile
from bs4 import BeautifulSoup
import pandas as pd
import json

#Производим разархивацию файла
archive = 'zip_var_19_1.zip'
z = ZipFile(archive)
list_file_html = [text_file.filename for text_file in z.infolist()] #Список названий разархивированных файлов

with ZipFile(archive, 'r') as zip_file:
    zip_file.extractall()

#Создаем функцию для парсинга страницы
def parser_html(file_html):
    with open(file_html, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        soup = BeautifulSoup(text, "html.parser")

        item = dict()

        item['Город'] = soup.find_all("span")[0].text.split(":")[1].strip()
        item['Строение'] = soup.find_all("h1", class_="title")[0].text.split(":")[1].strip()
        item['Улица'] = soup.find_all("p", class_="address-p")[0].text.split('Индекс')[0].split(":")[1].strip()
        item['Индекс'] = soup.find_all("p", class_="address-p")[0].text.split(":")[-1].strip()
        item['Этажность'] = int(soup.find_all(class_="floors")[0].text.split(":")[1].strip())
        item['Год постройки'] = soup.find_all(class_="year")[0].text.split(" ")[-1].strip()
        item['Наличие парковки'] = soup.find_all("span", string=re.compile("Парковка"))[0].text.split(":")[-1].strip() == "есть"
        item['Рейтинг'] = float(soup.find_all("span", string=re.compile("Рейтинг"))[0].text.split(":")[-1].strip())
        item['Просмотры'] = int(soup.find_all("span", string=re.compile("Просмотры"))[0].text.split(":")[-1].strip())
        item['Изображение'] = soup.find_all("img")[0]["src"]

        return item

#Добавляем спарсенные данные в общий список
items = []

for file_html in list_file_html:
    items.append(parser_html(file_html))

#Сортируем по просмотрам  и записываем в  файл
items = sorted(items, key=lambda x: x["Просмотры"], reverse=True)

with open("result_1.json", 'w', encoding="utf-8") as file_json:
    file_json.write(json.dumps(items))

#Фильтруем по году постройки и записываем в новый файл
with open("result_filter_1.json", 'w', encoding="utf-8") as file_json:
    filter_items = []

    for info in items:
        if info["Год постройки"] != "":
            info["Год постройки"] = int(info["Год постройки"])
            if info["Год постройки"] > 1990:
                filter_items.append(info)

    filter_items = sorted(filter_items, key=lambda x: x['Год постройки'], reverse=True)

    file_json.write(json.dumps(filter_items))

#Для числового поля 'Просмотры' считаем : максимальное и минимальное значения, среднее арифметическое,
# сумму, стандартное отклонение
item_views = dict()

df = pd.DataFrame.from_dict(items)

item_views["max_views"] = df["Просмотры"].max()
item_views["min_views"] = df["Просмотры"].min()
item_views["mean_views"] = df["Просмотры"].mean()
item_views["sum_views"] = df["Просмотры"].sum()
item_views["std_views"] = df["Просмотры"].std()

print("Статистические данные поля 'Просмотры':")

for key, value in item_views.items():
    print(key, value)

print("____________")

#рассчитайте частоту встречаемости для текстового поля 'Город'
cont_city = df['Город'].value_counts()

print("Частота встречаемости для числового поля 'Город':")
print(cont_city)
