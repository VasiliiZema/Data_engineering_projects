from zipfile import ZipFile
from bs4 import BeautifulSoup
import pandas as pd
import json

#Производим разархивацию файла
archive = 'zip_var_19_2.zip'
z = ZipFile(archive)
list_file_html = [text_file.filename for text_file in z.infolist()] #Список названий разархивированных файлов


with ZipFile(archive, 'r') as zip_file:
    zip_file.extractall()

#Создаем функцию для парсинга страницы
def parser_html(file_html):
    items = []
    with open(file_html, encoding="utf-8") as file:
        text = ""
        for row in file.readlines():
            text += row

        soup = BeautifulSoup(text, "html.parser")
        devaises = soup.find_all(class_="product-item")

        for devais in devaises:
            item = dict()

            item['id'] = devais.a['data-id'].strip()
            item['link'] = devais.find_all("a")[-1]['href'].strip()
            item['img_url'] = devais.find_all("img")[0]['src'].strip()
            item['title'] = devais.find_all("span")[0].text.strip()
            item['price'] = int(devais.find_all("price")[0].text.replace("₽","").replace(" ","").strip())
            item['bonus'] = devais.strong.text.strip()
            info_devais = devais.find_all("li")
            for info in info_devais:
                item[info["type"]] = info.text.strip()

            items.append(item)

    return items

#Добавляем спарсенные данные в общий список
html_list = []
for html_site in list_file_html:
    html_list += parser_html(html_site)


#Сортируем по ценам  и записываем в  файл
html_list = sorted(html_list, key=lambda x: x["price"], reverse=True)

with open("result_2.json", 'w', encoding="utf-8") as file_json:
    file_json.write(json.dumps(html_list))


#Фильтруем по памяти девайса (> 8 GB) и записываем в новый файл
with open("result_filter_2.json", 'w', encoding="utf-8") as file_json:
    filter = []

    for info in html_list:
        if "ram" in info:
            info["ram"] = int(info["ram"].replace("GB", "").strip())
            if info["ram"] > 8:
                filter.append(info)

    filter = sorted(filter, key=lambda x: x['ram'], reverse=True)

    file_json.write(json.dumps(filter))

#Для числового поля 'price' считаем : максимальное и минимальное значения, среднее арифметическое,
# сумму, стандартное отклонение
item_views = dict()

df = pd.DataFrame.from_dict(html_list)

item_views["max_price"] = df["price"].max()
item_views["min_price"] = df["price"].min()
item_views["mean_price"] = df["price"].mean()
item_views["sum_price"] = df["price"].sum()
item_views["std_price"] = df["price"].std()

print("Статистические данные поля 'price':")

for key, value in item_views.items():
    print(key, value)

print("____________")

#рассчитайте частоту встречаемости для текстового поля 'matrix'
cont_city = df['matrix'].value_counts()

print("Частота встречаемости для числового поля 'matrix':")
print(cont_city)

