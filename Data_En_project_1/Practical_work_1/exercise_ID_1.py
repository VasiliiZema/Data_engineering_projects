with open("text_1_var_19") as file:
    text = file.readlines()

word_dict = dict()

for str_ in text:
    line = list(str_.strip()
                .replace("!", " ")
                .replace("?", " ")
                .replace(",", " ")
                .replace(".", " ")
                .split(" ")
            )

    for word in line:
        if word in word_dict:
            word_dict[word] += 1
        else:
            word_dict[word] = 1
word_dict = dict(sorted(word_dict.items(), reverse=True, key = lambda x: x[1]))

with open("result_text_1.txt", "w") as result_file:
    for element in word_dict:
        result_file.write(element + ":" + str(word_dict[element]) + "\n")