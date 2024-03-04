import csv

result_list = []
filtr_list = []
sum_salary = 0

with open("text_4_var_19", newline="\n", encoding="utf-8") as file:
    text = csv.reader(file, delimiter=',')

    for row in text:
        data_person = {'id':int(row[0]),
                       'name':row[1] + ' ' + row[2],
                       'age':int(row[3]),
                       'salary':int(row[4][:-1])
                       }
        result_list.append(data_person)
        sum_salary += data_person['salary']

avr_salary = sum_salary / len(result_list)

for row in result_list:
    if row['salary'] > avr_salary and row['age'] > (25 +19):
        filtr_list.append(row)

filtr_list = sorted(filtr_list, key=lambda x: x['id'])

with open("result_text_4.csv", "w", encoding="utf-8", newline='\n') as text_result:
    text_writer = csv.writer(text_result, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for string_ in filtr_list:
        text_writer.writerow(string_.values())