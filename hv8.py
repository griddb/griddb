from functools import wraps

def type_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        num_list = [el for el in (*args, *kwargs.values())]
        n = [f"{func.__name__}({el}: {type(el)})" for el in num_list]
        print(*n, *func(*args, **kwargs), sep=",\n")

    return wrapper

@type_logger
def calc_cube(*x, **y):
    num_list = [el for el in (*x, *y.values()) if isinstance(el, int) or isinstance(el, float)]
    return [i ** 3 for i in num_list]

a = calc_cube(5, 11, 8.9, 34, ui=89, io=6)


def val_checker(l_func):
    def _val_checker(func):
        def wrapper(num):
            if l_func(num):
                print(func(num))
            else:
                raise ValueError(f"wrong val {num}")

        return wrapper
    return _val_checker

@val_checker(lambda x: x > 0)
def calc_cube(x):
    return x ** 3

try:
    a = calc_cube(5)
except ValueError as err:
    print(err)