def translate(num):
    angtr = {"ноль": "zero", "один": "one", "два": "two", "три": "three", "четыре": "four", "пять": "five",
             "шесть": "six", "семь": "seven", "восемь": "eight", "девять": "nine", "десять": "ten"}
    if digit in angtr:
        print(angtr[digit])
    elif digit[0].lower() + digit[1:] in angtr:
        print(angtr[digit.lower()].capitalize())
    elif digit.lower() in angtr:
        print(angtr[digit.lower()])
    else:
        return None


digit = input()
translate(digit)
