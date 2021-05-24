from random import choice, randrange


def jokes(n, repeat=False):
    a = ["автомобиль", "лес", "огонь", "город", "дом"]
    b = ["сегодня", "вчера", "завтра", "позавчера", "ночью"]
    c = ["веселый", "яркий", "зеленый", "утопичный", "мягкий"]

    no, adv, adj = a.copy(), b.copy(), c.copy()
    lofj = []
    list_min = min(no, adv, adj)

    while n and len(list_min):
        num = randrange(len(list_min))
        if repeat:
            lofj.append(f'{no.pop(num)} {adv.pop(num)} {adj.pop(num)}')
        else:
            lofj.append(f'{choice(a)} {choice(b)} {choice(c)}')
        n -= 1
    return lofj


print(jokes(10, True))