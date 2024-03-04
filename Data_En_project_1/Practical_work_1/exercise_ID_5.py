from bs4 import BeautifulSoup
import csv

result_list = []

with open("text_5_var_19", encoding="utf-8") as file:
    text = file.readlines()

    text_html = ''
    for line in text:
        text_html += line

    soup = BeautifulSoup(text_html, "html.parser")
    string_tr = soup.find_all('tr')
    string_tr = string_tr[1:]

    for row in string_tr:
        string_td = row.find_all('td')
        item = {
            'company':string_td[0].text,
            'contact':string_td[1].text,
            'countre':string_td[2].text,
            'price':string_td[3].text,
            'item':string_td[4].text
                }
        result_list.append(item)

with open("result_text_5.csv", "w", encoding="utf-8", newline='\n') as text_result:
    text_writer = csv.writer(text_result, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for string_ in result_list:
        text_writer.writerow(string_.values())

