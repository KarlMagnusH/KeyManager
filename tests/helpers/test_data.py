from itertools import product
from typing import Dict, Iterable, List, Optional, Tuple
import pandas as pd


class CaseLists:
    def __init__(self) -> None:
        self.groups: Dict[str, List] = {}
        self.cols: list[str] = []

    def add(self, name: str, values: Iterable) -> "CaseLists":
        self.groups[name] = list(values)
        return self

    def combine(self, *columns: Optional[List[str]], mode: str = "cartesian") -> "CaseLists":
        """Combine groups into a DataFrame.

        mode:
          - 'cartesian' (default): Cartesian product of all lists
          - 'zip': pairwise by index (requires equal lengths)
        """
        if not columns:
            self.cols = list(self.groups.keys())
        else:
            for c in columns:
                if c not in self.groups:
                    raise KeyError(f"Unknown group '{c}'")
                else: 
                    self.cols.append(c)

        if mode == "cartesian":
            iterables = [self.groups[c] for c in self.cols]
            self.rows = [tuple(item) for item in product(*iterables)]
        elif mode == "zip":
            lengths = {c: len(self.groups[c]) for c in self.cols}
            if len(set(lengths.values())) != 1:
                raise ValueError(f"All groups must have same length for 'zip' mode. {lengths}")
            self.rows = list(zip(*(self.groups[c] for c in self.cols)))
        else:
            raise ValueError("mode must be 'cartesian' or 'zip'")
        
        return self
    
    def get_df(self):
        return pd.DataFrame(self.rows, columns=self.cols)
    
    def get_groups(self, *keys: list[str]) -> dict:
        if not keys:
            return self.groups
        else:
            return {name: self.groups[name] for name in keys}

BUILTINS: Dict[str, Dict[str, List]] = {
    "str": {
        "mixed": ["alice", "alice", " tim ", "tim", None, 1, 7.5, ["1", "2"], {"1", "2"}, {"1": 1, "2": 2}],
        "normal": ["alice", "alice", "tim", "tim", "bab", "kim", "lucie", "becie", "cris", "annie"],
        "dif": ["alice", "tim", "bab", "kim", "lucie", "becie", "cris", "annie", "tæp", "lis"],
        "like": ["ens"] * 10,
        "none": [None] * 10,
    },
    "int": {
        "mixed": [1, 1, 0, -1, None, 7.5, "3", [1, 2], {1, 2}, {1: 1, 2: 2}],
        "normal": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        "dif": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "like": [42] * 10,
        "none": [None] * 10,
    },
}

if __name__ == "__main__":
    c = (CaseLists()
        .add("str_mixed", BUILTINS["str"]["mixed"])
        .add("int_normal", BUILTINS["int"]["normal"])
        .add("int_normal1", BUILTINS["int"]["normal"])
    )
    df_c = c.combine().get_df()
    print(df_c)
    groups = c.combine().get_groups()
    print(groups)

    df_c_zip = c.combine(mode="zip").get_df()
    print(df_c_zip)
    zipped_groups = c.combine(mode="zip").get_groups()
    print(zipped_groups)

    df_c_error = c.combine(mode="pippi").get_df()
    print(df_c_zip)