import random
from typing import Any, Union

Element = Any
Weight = Union[int, float]
WeightedElements = dict[Element, Weight]

def weighted_choice(elements: WeightedElements) -> Element:
    population, weights = list(zip(*elements.items()))
    return random.choices(population, weights)[0]

def random_span(seq_len: int, min_size: int, max_size: int) -> tuple[int, int]:
    """
    Returns a tuple (start, stop) that corresponds to a span in a sequence
    of length `seq_len`.
    """
    if min_size > seq_len:
        raise ValueError("seq_len can't be smaller than min_size")
    if min_size == seq_len:
        return 0, seq_len
    start = random.randrange(seq_len - min_size)
    size = random.randint(min_size, max_size)
    stop = min(start + size, seq_len)
    return start, stop

def random_spans(seq_len, n_spans, min_size, max_size):
    """
    Returns `n_spans` non-overlapping tuples (start, stop).
    """
    ret = []
    indices = positive_integers_with_sum(n_spans, seq_len - n_spans * max_size)
    indices = [i + max_size for i in indices]
    pos = 0
    for i in indices:
        start = random.randint(pos, i + pos - min_size)
        size = random.randint(min_size, max_size)
        stop = min(start + size, i + pos)
        ret.append((start, stop))
        pos += i
    return ret

def integers_with_sum(n, total):
    """
    Returns n integers 0 or greater that sum to total, in random order.
    """
    if n <= 0 or total <= 0:
        raise ValueError
    ret = positive_integers_with_sum(n, total + n)
    return [i-1 for i in ret]

def positive_integers_with_sum(n, total):
    """
    Returns n integers greater than 0 that sum to total, in random order.
    """
    if n <= 0 or total <= 0:
        raise ValueError
    ls = [0]
    ret = []
    while len(ls) < n:
        c = random.randint(1, total)
        if c in ls:
            continue
        ls.append(c)
    ls.sort()
    ls.append(total)
    for i in range(1, len(ls)):
        ret.append(ls[i] - ls[i-1])
    return ret

def read_tsv_mapping(filename):
    mapping = {}
    with open(filename) as f:
        for line in f:
            key, value = line.strip().split('\t')
            mapping[key] = value
    return mapping
