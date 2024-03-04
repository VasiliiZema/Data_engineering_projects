import numpy as np
import json

matrix = np.load('matrix_19.npy')

result_dict = dict()

result_dict['sum'] = 0
result_dict['avr'] = 0
result_dict['sumMD'] = 0
result_dict['avrMD'] = 0
result_dict['sumSD'] = 0
result_dict['avrSD'] = 0
result_dict['max'] = matrix[0][0]
result_dict['min'] = matrix[0][0]

leng = len(matrix)

for i in range(0, leng):
    for j in range(0, leng):
        result_dict['sum'] += matrix[i][j]
        if i == j:
            result_dict['sumMD'] += matrix[i][j]
        if (i + j) == (leng - 1):
            result_dict['sumSD'] += matrix[i][j]
        result_dict['max'] = max(result_dict['max'], matrix[i][j])
        result_dict['min'] = min(result_dict['min'], matrix[i][j])

result_dict['avr'] = result_dict['sum'] / (leng * leng)
result_dict['avrMD'] = result_dict['sumMD'] / leng
result_dict['avrSD'] = result_dict['sumSD'] / leng

for key in result_dict.keys():
    result_dict[key] = float(result_dict[key])

with open('result_1_19.json', 'w') as file:
    file.write(json.dumps(result_dict))

norm_matrix = np.ndarray((leng, leng), dtype=float)

for i in range(0, leng):
    for j in range(0, leng):
        norm_matrix[i][j] = matrix[i][j] / result_dict['sum']

np.save('result_norm_matrix_1_19', norm_matrix)



