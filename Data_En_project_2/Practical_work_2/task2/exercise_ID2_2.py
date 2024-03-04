import os.path
import numpy as np

matrix = np.load('matrix_19_2.npy')

leng = len(matrix)
x = []
y = []
z = []

variant = 19

for i in range(0, leng):
    for j in range(0, leng):
        if matrix[i][j] > (500 + variant):
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])

np.savez('result_2_19', x=x, y=y, z=z)
np.savez_compressed('result_zip_2_19', x=x, y=y, z=z)

print(f"result_2_19 = {os.path.getsize('result_2_19.npz')}")
print(f"result_zip_2_19 = {os.path.getsize('result_zip_2_19.npz')}")