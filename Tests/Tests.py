import pytest
from Unit_tests import (
    is_palindrome,
    fibonacci,
    count_vowels,
    calculate_discount,
    flatten_list,
    word_frequencies,
    is_prime,
)
# 1. is_palindrome(text)
@pytest.mark.parametrize(
    "text, expected",
    [
        ("kajak", True),
        ("Kobyła ma mały bok", True),
        ("python", False),
        ("", True),
        ("A", True),
    ],
)
def test_is_palindrome(text, expected):
    assert is_palindrome(text) == expected

# 2. fibonacci(n)
@pytest.mark.parametrize(
    "n, expected",
    [
        (0, 0),
        (1, 1),
        (5, 5),
        (10, 55),
    ],
)
def test_fibonacci_basic(n, expected):
    assert fibonacci(n) == expected


def test_fibonacci_negative():
    try:
        result = fibonacci(-1)
    except ValueError:
        return
    assert result is None

# 3. count_vowels(text)
@pytest.mark.parametrize(
    "text, expected",
    [
        ("Python", 2),
        ("AEIOUY", 6),
        ("bcd", 0),
        ("", 0),
        ("Próba żółwia", 5),
    ],
)
def test_count_vowels(text, expected):
    assert count_vowels(text) == expected

# 4. calculate_discount(price, discount)
@pytest.mark.parametrize(
    "price, discount, expected",
    [
        (100, 0.2, 80.0),
        (50, 0, 50.0),
        (200, 1, 0.0),
    ],
)
def test_calculate_discount_valid(price, discount, expected):
    assert calculate_discount(price, discount) == expected

@pytest.mark.parametrize("price, discount", [(100, -0.1), (100, 1.5)])
def test_calculate_discount_invalid(price, discount):
    with pytest.raises(ValueError):
        calculate_discount(price, discount)

# 5. flatten_list(nested_list)
@pytest.mark.parametrize(
    "nested, expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ([1, [2, 3], [4, [5]]], [1, 2, 3, 4, 5]),
        ([], []),
        ([[[1]]], [1]),
        ([1, [2, [3, [4]]]], [1, 2, 3, 4]),
    ],
)
def test_flatten_list(nested, expected):
    assert flatten_list(nested) == expected

# 6. word_frequencies(text)
@pytest.mark.parametrize(
    "text, expected",
    [
        ("To be or not to be", {"to": 2, "be": 2, "or": 1, "not": 1}),
        ("Hello, hello!", {"hello": 2}),
        ("", {}),
        ("Python Python python", {"python": 3}),
        (
            "Ala ma kota, a kot ma Ale.",
            {"ala": 1, "ma": 2, "kota": 1, "a": 1, "kot": 1, "ale": 1},
        ),
    ],
)
def test_word_frequencies(text, expected):
    assert word_frequencies(text) == expected

# 7. is_prime(n)
@pytest.mark.parametrize(
    "n, expected",
    [
        (2, True),
        (3, True),
        (4, False),
        (0, False),
        (1, False),
        (5, True),
        (97, True),
    ],
)
def test_is_prime(n, expected):
    assert is_prime(n) == expected
