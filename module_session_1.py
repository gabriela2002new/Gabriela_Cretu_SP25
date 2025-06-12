from typing import List

import numpy as np
from defusedxml.lxml import tostring
from jedi.inference.utils import to_list


def task_1(array: List[int], target: int) -> List[int]:
    for i, num in enumerate(array):
        if target - num in array[i+1:]:
            return [num, target - num]
    return []


def task_2(number: int) -> int:
    # Remove trailing zeros
    mirror_number=0
    negative=False
    if number<0:
        negative=True
        number=np.abs(number)
    while number % 10 == 0 and number != 0:
        number = number // 10

    while number != 0:
        digit = number % 10
        mirror_number = mirror_number * 10 + digit
        number = number // 10

    if negative:
        mirror_number=-mirror_number

    return mirror_number









from typing import List

def task_3(array: List[int]) -> int:
    n = len(array)
    min_second_index = n + 1  # something larger than any index
    duplicate = -1

    for i in range(n):
        for j in range(i + 1, n):
            if array[i] == array[j] and j < min_second_index:
                min_second_index = j
                duplicate = array[i]

    return duplicate



def task_4(string: str) -> int:
    custom_order = "IVXLCDM"
    array = [1, 5, 10, 50, 100, 500, 1000]

    order_map = {char: i for i, char in enumerate(custom_order)}
    number_map = {char: val for char, val in zip(custom_order, array)}

    number = 0
    for i in range(len(string) - 1):
        if order_map[string[i]] < order_map[string[i + 1]]:
            number -= number_map[string[i]]
        else:
            number += number_map[string[i]]

    number += number_map[string[-1]]  # Add the last character's value

    return number


def task_5(array: List[int]) -> int:
    minimum = array[0]
    for i in array:
        if i < minimum:
            minimum = i
    return minimum
