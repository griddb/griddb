seconds = int(input("Введите секунды: "))


time = 0
minute = 60
hour = 3600
day = 3600 * 24
month = day * 28
year: int = month * 12
tyears = year * 1000

while True:
    if seconds >= tyears:
        time = seconds // tyears
        seconds %= tyears
        print(f'{time} тлет.', end=" ")
    if seconds >= year:
        time = seconds // year
        seconds %= year
        print(f'{time} лет', end=" ")
    elif seconds >= month:
        time = seconds // month
        seconds %= month
        print(f'{time} мес.', end=" ")
    elif seconds >= day:
        time = seconds // day
        seconds %= day
        print(f'{time} дн.', end=" ")
    elif seconds >= hour:
        time = seconds // hour
        seconds %= hour
        print(f'{time} час.', end=" ")
    elif seconds >= minute:
        time = seconds // minute
        seconds %= minute
        print(f'{time} мин.', end=" ")
    elif seconds < minute:
        print(f'{seconds} сек.')
        seconds = 0
        break
