import pandas as pd
import pytest

antal_test_cases = 10
for key, values in str_case.update(int_case):
    assert isinstance(str, key)
    assert len(values) == antal_test_cases
"""
    Skal teste håndtering af strings:
    name (str), None=False
    age (int)

    case hvor:
    - name er str
    - name er der er i forvejen
    - name er str med initial mellemrum
    - name er None
    - eksisterende navn uden mellemrum
    - name er int
    - name er float
"""

str_case = {
    "mixed": ["alice", "alice", " tim ", "tim", None, 1, 7.5, ["1", "2"], {"1", "2"}, {"1": 1, "2": 2}],
    "normal": ["alice", "alice", "tim", "tim", "bab", "kim", "lucie", "becie", "cris", "annie"],
    "forskellige": ["alice", "tim", "bab", "kim", "lucie", "becie", "cris", "annie", "tæp", "lis"],
    "ens": ["ens"] * antal_test_cases,
    "None": [None] * antal_test_cases,
    }
"""
    Skal teste håndtering af strings:
    name (str), None=False

    case hvor værdier:
    - str
    - der er der i forvejen
    - str med initial mellemrum
    - eksisterende navn uden mellemrum
    - None
    - er int
    - er float
    - iterator: list
    - iterator: set
    - iterator: dict
"""

str_case = {
    "mixed": ["alice", "alice", " tim ", "tim", None, 1, 7.5, ["1", "2"], {"1", "2"}, {"1": 1, "2": 2}],
    "normal": ["alice", "alice", "tim", "tim", "bab", "kim", "lucie", "becie", "cris", "annie"],
    }

@pytest.fixture
def dim_person():
    """
    Skal teste håndtering af strings:
    name (str), None=False
    age (int)

    case hvor:
    - name er str
    - name er der er i forvejen
    - name er str med initial mellemrum
    - name er None
    - eksisterende navn uden mellemrum
    - name er int
    - name er float
    """
    names = ["alice", "alice", " tim ", "tim", None, 1, 7.5, ]
    return pd.DataFrame({
        "navn_str": names,
        "alder_int": [i for i in range(len(names)+1)]
    })

@pytest.fixture
def dim_geo():
    ""
    return pd.DataFrame({
        "by_str": [" cph", "BOB", "Brons ", None, "Brons", None, "chavn",],
        "zipcode_int": [" 142", "1415", "099 ", None, None, , "Brons ", 1415,]
    })

@pytest.fixture
def fact_school():
    return pd.DataFrame({
        "key_geo": [1, 2, 3],
        "name": [" alice ", "BOB", "ChArLiE "]
    })

@pytest.fixture