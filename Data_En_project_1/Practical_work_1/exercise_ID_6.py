import json
from bs4 import BeautifulSoup
import requests

url = 'https://whiskyhunter.net/api/auctions_info'

req = requests.get(url)

# Преобразуем полученные данные в json формат
data = json.loads(req.text)

soup = BeautifulSoup(features="html.parser")
table = soup.new_tag("table") # Создаем новый тег <table>....</table>
tr = soup.new_tag("tr")# Создаем новый тег <tr>....</tr>

# Добавляем название заголовков для таблицы
for key in data[0]:
    th = soup.new_tag("th")# Создаем новый тег <th>....</th>
    th.string = key# Записываем значение для названия столбца в <th>....</th>
    tr.append(th)# Добавляем значение в <th>....</th> в <tr>....</tr>
table.append(tr)# Добавляем значение в <tr>....</tr> в <table>....</table>

# Добавляем значения в столбцы таблицы
for row in data:
    tr = soup.new_tag("tr")# Создаем новый тег <tr>....</tr>
    for key, val in row.items():
        td = soup.new_tag("td")# Создаем новый тег <td>....</td>
        td.string = str(val)# Записываем значение для названия столбца в <td>....</td>
        tr.append(td)# Добавляем значение в <td>....</td> в <tr>....</tr>
    table.append(tr)# Добавляем значение в <tr>....</tr> в <table>....</table>

soup.append(table)
html_table = soup.prettify()# Преобразуем в html
# Создаем и записываем в html файл
with open("text_6_var_19.html", "w", encoding="utf-8") as f:
    for line in html_table:
        f.write(line)

