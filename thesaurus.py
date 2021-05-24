def thesaurus(*args):
    flst = []
    for i in range(len(args)):
        lst = []
        for j in range(len(args)):
            if args[i][0] == args[j][0]:
                lst.append(args[j])
        if lst not in flst:
            flst.append(lst)
    return flst

print(thesaurus("Иван", "Мария", "Петр", "Илья"))
