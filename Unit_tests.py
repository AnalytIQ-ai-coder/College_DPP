import pytest
import string
import re
from collections import Counter

def is_palindrome(text: str) -> bool:
    cleaned_text = ''.join(text.split()).lower()
    return cleaned_text == cleaned_text[::-1]
def fibonacci(n: int) -> int:
    if n < 0:
        raise ValueError("Number cannot be negative")
    if n == 0:
        return 0
    if n == 1:
        return 1
    a, b = 0, 1
    for i in range(2, n + 1):
        a, b = b, a + b
    return b
def count_vowels(text: str) -> int:
    cleaned_text = ''.lower()
    vowels = set("aeiouy")
    return sum(1 for ch in cleaned_text if ch in vowels)

def calculate_discount(price: float, discount: float) -> float:
    if discount > 1:
        raise ValueError("Number cannot be greater than one")
    if discount < 0:
        raise ValueError("Number cannot be negative")
    return round(price * (1-discount))
def flatten_list(nested_list: list) -> list:
    result = []
    for item in nested_list:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result

def word_frequencies(text: str) -> dict:
    text = text.lower()
    text = re.sub(r"[^a-z0-9ąćęłńóśżź]+", " ", text)
    words = text.split()
    return dict(Counter(words))

def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n in (2, 3):
        return True
    if n % 2 == 0:
        return False
    limit = int(n**0.5) + 1
    for i in range(3, limit, 2):
        if n % i == 0:
            return False
    return True