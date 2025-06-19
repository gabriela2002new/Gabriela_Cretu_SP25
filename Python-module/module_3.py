import time
from typing import List

from numpy.lib.function_base import delete

Matrix = List[List[int]]


def task_1(exp: int):
    def power_factory(base:int):
        return base**exp
    return power_factory


def task_2(*args, **kwargs):
    print(args)
    print(kwargs)


def helper(func):
    def wrapper(name):
        print("Hi, friend! What's your name?")
        func(name)
        print("See you soon!")
    return wrapper


@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")


def timer(func):
    def wrapper():
        now=time.time()
        func()
        new=time.time()
        passed_time=new-now
        print(f"Finished task 4 in {passed_time} secs")
    return wrapper


@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])


def task_5(matrix: Matrix) -> Matrix:
    return matrix.T


def task_6(queue: str):
    string1="("
    string2=")"
    pair=string1+string2

    while pair in queue:
        queue = queue.replace(pair, '', 1)  # replace only the first occurrence each time
    return len(queue)==0
