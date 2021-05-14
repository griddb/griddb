my_list = []

qsum = 0
sum = 0
n1 = 1

for i in range(1001):
    if i % 2 == 1:
        my_list.append(i**3)

for n in my_list:
    a = n
    sum = 0
    while a != 0:
        sum += a % 10
        a = a // 10
    if sum % 7 == 0:
        qsum += n

print(qsum)

qsum = 0
sum = 0

for n in my_list:
    n += 17
    a = n
    sum = 0
    while a != 0:
        sum += a % 10
        a = a // 10
    if sum % 7 == 0:
        qsum += n

print(qsum)