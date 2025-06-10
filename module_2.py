# from collections import defaultdict as dd
# from itertools import product
from itertools import combinations
from typing import Any, Dict, List, Tuple


from typing import Dict

from numpy.ma.core import empty


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]) -> Dict[str, int]:
    data_3 = {}
    all_keys = set(data_1.keys()) | set(data_2.keys())  # Union of keys

    for key in all_keys:
        if key in data_1 and key in data_2:
            data_3[key] = data_1[key] + data_2[key]
        elif key in data_1:
            data_3[key] = data_1[key]
        else:
            data_3[key] = data_2[key]

    return data_3



def task_2():
    dict_consecutive={}
    square=lambda a: a**2

    for i in range(1,16):
        key=i
        value=square(i)
        dict_consecutive[key]=value
    return dict_consecutive


from typing import Dict, Any, List


def task_3(data: Dict[Any, List[str]]) -> List[ str]:
    letters = []
    keys = list(data.keys())
    keys_combinations = list(range(len(data[keys[0]])))

    # Initialize combinations with the first key's values
    combinations = {i: data[keys[0]][i] for i in keys_combinations}

    for key in keys[1:]:
        new_combinations = {}
        p = 0
        for j in combinations:
            for i in range(len(data[key])):
                new_combinations[p] = combinations[j] + data[key][i]
                p += 1
        combinations = new_combinations  # Update with new layer of combinations

    return list(combinations.values())



from typing import Dict

def task_4(data: Dict[str, int]):
    max1 = float('-inf')
    max2 = float('-inf')
    max3 = float('-inf')
    key_max1 = key_max2 = key_max3 = None

    for key, val in data.items():
        if val > max1:
            max3, key_max3 = max2, key_max2
            max2, key_max2 = max1, key_max1
            max1, key_max1 = val, key
        elif val > max2:
            max3, key_max3 = max2, key_max2
            max2, key_max2 = val, key
        elif val > max3:
            max3, key_max3 = val, key

    result = []
    if key_max1 is not None:
        result.append(key_max1)
    if key_max2 is not None:
        result.append(key_max2)
    if key_max3 is not None:
        result.append(key_max3)

    return result






def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    new_dict = {}
    for key, val in data:
        # Initialize list if key not present
        if key not in new_dict:
            new_dict[key] = []
        # Append val to the list for this key
        new_dict[key].append(val)
    return new_dict



def task_6(data: List[Any]):
    deleted_elem=[]
    for elem in data:
        if elem in deleted_elem:
            data.remove(elem)
        else:
            deleted_elem.append(elem)

    return deleted_elem




def task_7(words: List[str]) -> str:
    lengths = [len(word) for word in words]
    min_length = min(lengths)
    i = lengths.index(min_length)
    word1 = words[i]

    for end in range(len(word1), 0, -1):  # Try longer prefixes first
        substring = word1[:end]
        found = True
        for j, word2 in enumerate(words):
            if j != i and  not  word2.startswith(substring):
                found = False
                break
        if found:
            return substring

    return ""




def task_8(haystack: str, needle: str) -> int:
    if needle == "":
        return 0
    if needle in haystack:
        return haystack.index(needle)
    return -1
