broken_sentence = ('в', '5', 'часов', '17', 'минут', 'температура', 'воздуха', 'была', '+5', 'градусов')
correct_sentence = []

for i in broken_sentence:
    if i.replace("+", "").replace("-", "").isdigit():
        if i.isdigit():
            correct_sentence.append(f"'{int(i):02}'")
        else:
            correct_sentence.append(f"'{i[0]}{int(i[1:]):02}'")
    else:
        correct_sentence.append(i)

print(correct_sentence)
print(" ".join(correct_sentence))
