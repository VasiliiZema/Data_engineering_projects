with open("text_2_var_19") as file:
    text = file.readlines()
    
with open("result_text_2.txt", "w") as result_file:
    for row in text:
        line = list(map(int, row.split(":")))
        average = sum(line) / len(line)
        result_file.write(str(round(average, 2)) + "\n")