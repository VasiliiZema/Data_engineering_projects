import math
with open("text_3_var_19") as file:
    text = file.readlines()

list_result = []
for row in text:
    index = 0
    list_element = []
    list_row = row.split(",")

    for element in list_row:
        if element != "NA":
            if math.sqrt(int(element)) > (50 + 19):
                list_element.append(element.strip())
        else:
            new_element = (int(list_row[index-1]) + int(list_row[index +1])) / 2
            if math.sqrt(new_element) > (50 + 19):
                list_element.append(str(new_element))
        index += 1

    list_result.append(list_element)

with open("result_text_3.txt", "w") as text_result:
    for string_ in list_result:
        text_result.write(",".join(string_) + "\n")


