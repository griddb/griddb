percents = 20

for i in range(percents+1):
    if 10 < i < 15 or 10 < i % 100 < 15:
        print(f'{i} секунд')
    elif i % 10 == 1:
        print(f'{i} секунда')
    elif 1 < i % 10 < 5:
        print(f'{i} секунды')
    else:
        print(f'{i} секунд')
