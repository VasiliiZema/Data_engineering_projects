import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Session
import csv

headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }

base_url = "https://nur.kz/latest/"
s = Session()
s.headers.update(headers)
#Создаем JSON файл для записи отобранных данных
with open("result_news.json", "w", encoding="utf-8") as text_result:
    text_item = []
    count = 1 #Переменная счетчика для перехода по страницам после автозагрузки
    #count_url=0 #Переменная счетчика количества ссылок
    #count_offers = 0 #Переменная для подсчета количества предложений в датасете
    #limit_offers = int(input()) #ПЗадаем лимит по количеству предложений в датасете
    # Цикл прохождения по ссылкам и забора нужной информации
    while count < 10: #Пока число спарсенных страниц не превышает 10, идет цикл обработки данных
        if count > 1: #Если появилась ссылка после автозагрузки страницы
            base_url = "https://nur.kz/latest/" + str(count) #Добавляем переменную счетчика для перехода по ссылке после автозагрузки
        count += 1
        req = s.get(base_url) #Переходим по ссылке
        src = req.text # Сохраняем html код в переменную

        # При помощи библиотеки BeautifulSoup парсим необходимые нам данные
        soup = BeautifulSoup(src, "lxml")
        all_news = soup.find_all(class_="article-preview-category__content") #Находим все ссылки на загруженной странице
        url_list = []
        #Цикл добавления ссылок в список
        for item in all_news:
            link_new = item.get('href')
            url_list.append(link_new)


        #Проходимся по ссылкам и забираем нужную информацию
        for news in url_list:
            if "special" not in str(news): #Избавляемся от ссылок где есть слово "special" в названии ссылки, они не поддаются общему парсингу
                req = requests.get(news).text
                soup = BeautifulSoup(req, "lxml")
                #Находим заголовок новости
                title = soup.find("h1", class_="main-headline js-main-headline").text.strip()
                #Находим названия категорий и тегов(сохраняются в список)
                class_news = soup.find("ul", class_="breadcrumbs").find_all("a")

                if len(class_news) > 1: #Если список больше одного элемента, значит в нем есть и категория и тег новости
                    news_class = class_news[0].text.strip() #Присваиваем переменной название категории новости
                    news_teg = class_news[1].text.strip() #Присваиваем переменной название тега новости
                else: #Если список из одного элемента, значит в нем есть категория, но нет тега новости
                    news_class = class_news[0].text.strip() #Присваиваем переменной название категории новости
                    news_teg = 'Na' #Присваиваем переменной название "Na" тега новости, т.к. его нет
                # Находим дату добавления новости на страницу
                date = soup.find("time", class_="datetime datetime--publication").text.strip(
                ).replace("Сегодня,", "").replace("Вчера,", "").strip()
                # Находим текст новости (добавляются абзацы в список)
                strong_text_news = soup.find_all("p", class_="align-left formatted-body__paragraph")

                text = ''
                # Цикл добавления абзацев в единый текст(в переменную text)
                for line in strong_text_news:
                    text += (' ' + line.text.replace(" ", ""))

            #count_offers += len(text.split(".")) # Подсчет количества предложений добавленных в датасет
            # Создаем словарь для выбранных данных
            item = {"url":news.strip(),
                    "title":title,
                    "content":text,
                    "date":date,
                    "catecory":news_class,
                    "tag":news_teg
                        }
            # Добавляем данные в список
            text_item.append(item)

    # Сортируем данные по дате
    text_item = sorted(text_item, key=lambda x: x['date'], reverse=False)

    # Добавляем данные в JSON файл
    text_result.write(json.dumps(text_item))

# Фильтруем данные по полю "tag" == "Преступления"
filter_item = []
for el in text_item:
    if el["tag"] == "Преступления":
        filter_item.append(el)

# Добавляем отфильтрованные данные в JSON файл
with open("filter_news.json", "w", encoding="utf-8") as text_filter:
    text_filter.write(json.dumps(filter_item))

#Для текстового поля "catecory" посчитайте частоту меток

df = pd.DataFrame.from_dict(text_item)

catecory_count = df["catecory"].value_counts()

print("Частота встречаемости меток для поля 'catecory'")
print(catecory_count)
print("____________")

#Сгруппируем данные по полю "catecory", и посчитаем для получившегося числового поля "count_catecory" показатели статистики
count_catecory_item = dict()


count_catecory_item["max_count"] = df.groupby('catecory').count().reset_index()['tag'].max()
count_catecory_item["min_count"] = df.groupby('catecory').count().reset_index()['tag'].min()
count_catecory_item["mean_count"] = df.groupby('catecory').count().reset_index()['tag'].mean()
count_catecory_item["sum_count"] = df.groupby('catecory').count().reset_index()['tag'].sum()
count_catecory_item["std_count"] = df.groupby('catecory').count().reset_index()['tag'].std()

print("Статистические данные поля 'count_catecory':")

for key, value in count_catecory_item.items():
    print(key, value)

