# from collections import Counter
import os
import random
import re
from multiprocessing import connection
from pathlib import Path
# from random import choice
from random import seed
from typing import List, Union
from random import choice
import string

from numpy.core.defchararray import upper

import requests
from requests.exceptions import HTTPError, ConnectionError
#from gensim.utils import simple_preprocess


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES  = r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_5\names.txt"
PATH_TO_SURNAMES = r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_5\last_names.txt"
PATH_TO_OUTPUT = r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_5\sorted_names_and_surnames.txt"

PATH_TO_TEXT= r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_5\random_text.txt"
PATH_TO_STOP_WORDS= r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Session_5\stop_words.txt"


def task_1():
    seed(1)

    # Read and sort names
    with open(PATH_TO_NAMES, "r", encoding="utf-8") as file:
        names = [line.strip().lower() for line in file]
    names.sort()

    # Read surnames
    with open(PATH_TO_SURNAMES, "r", encoding="utf-8") as file:
        surnames = [line.strip().lower() for line in file]

    # Assign random surnames and combine
    full_names = [f"{name} {choice(surnames)}" for name in names]

    # Write to output file
    with open(PATH_TO_OUTPUT, "w", encoding="utf-8") as file:
        for full_name in full_names:
            file.write(f"{full_name}\n")


import string

import re

def task_2(top_k: int):
    with open(PATH_TO_STOP_WORDS, 'r', encoding='utf-8') as f:
        stop_words = set(line.strip().lower() for line in f)

    with open(PATH_TO_TEXT, 'r', encoding='utf-8') as f:
        text = f.read()

    # Use regex to extract clean lowercase words only
    words = re.findall(r'\b[a-z]+\b', text.lower())

    # Filter out stop words
    filtered_words = [word for word in words if word not in stop_words]

    # Count with dict
    word_freq_dict = {}
    for word in filtered_words:
        if word in word_freq_dict:
            word_freq_dict[word] += 1
        else:
            word_freq_dict[word] = 1

    # Get top_k as list of tuples
    top = sorted(word_freq_dict.items(), key=lambda item: item[1], reverse=True)[:top_k]
    return top


from requests.exceptions import HTTPError, ConnectionError, RequestException

import requests
from requests.exceptions import HTTPError, ConnectionError, RequestException

def task_3(url: str) -> requests.Response:

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except (HTTPError, ConnectionError) as e:
        raise RequestException(str(e)) from e




from typing import List, Union

def task_4(data: List[Union[int, str, float]]) -> float:
    try:
        total = sum(data)
    except TypeError:
        # Convert all elements to float if TypeError occurs
        data = [float(x) for x in data]
        total = sum(data)
    return total


def task_5():
    try:
        a, b = input().split()
        a = float(a)  # try converting here; raises ValueError if fails
        b = float(b)
    except ValueError:
        print("Entered value is wrong")
        return

    try:
        result = a / b
    except ZeroDivisionError:
        print("Can't divide by zero")
        return

    print(result)


