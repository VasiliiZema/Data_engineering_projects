import json
import msgpack
import os.path

with open('products_19.json') as file:
    data = json.load(file)

    result_dict = dict()

    for item in data:
        if item['name'] in result_dict:
            result_dict[item['name']].append(item["price"])
        else:
            result_dict[item['name']] = []
            result_dict[item['name']].append(item["price"])
    products_price = []

    for name, price in result_dict.items():
        products_price.append({
        'name':name,
        'avr_price':(sum(price)/len(price)),
        'max_price':max(price),
        'min_price':min(price)
        })

    with open('result_3_19.json', 'w') as file_json:
        file_json.write(json.dumps(products_price))

    with open('result_3_19.msgpack', "wb") as file_msgpack:
        file_msgpack.write(msgpack.dumps(products_price))

print(f"result_3_19.json = {os.path.getsize('result_3_19.json')}")
print(f"result_3_19.msgpack = {os.path.getsize('result_3_19.msgpack')}")
