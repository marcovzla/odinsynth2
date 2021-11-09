import random
from typing import Any, Union

Element = Any
Weight = Union[int, float]
WeightedElements = dict[Element, Weight]

def weighted_choice(elements: WeightedElements) -> Element:
    population, weights = list(zip(*elements.items()))
    return random.choices(population, weights)[0]
