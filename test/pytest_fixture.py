import pandas as pd
import pytest

from TestCaseGen import TestCaseGen, BUILTINS

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
        "zipcode_int": [" 142", "1415", "099 ", None, None, "Brons ", 1415,]
    })

@pytest.fixture
def fact_school():
    return pd.DataFrame({
        "key_geo": [1, 2, 3],
        "name": [" alice ", "BOB", "ChArLiE "]
    })

if __name__ == "__main__":
    print(BUILTINS["str"]["mixed"])