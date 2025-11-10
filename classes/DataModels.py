from typing import List, Generic, TypeVar, Dict
from enum import Enum
from math import ceil

T = TypeVar("T")

class Operation(Enum):
    EQ = "="
    NEQ = "<>"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="

class Condition:
    def __init__(self, column: str, operation: Operation, operand: int | str) -> None:
        self.column: str = column
        self.operation: Operation = operation
        self.operand: int | str = operand

class DataRetrieval:
    def __init__(self, table: List[str], column: List[str], conditions: List[Condition]) -> None:
        self.table: List[str] = table
        self.column: List[str] = column
        self.conditions: List[Condition] = conditions

class DataWrite(Generic[T]):
    def __init__(self, table: List[str], column: List[str], conditions: List[Condition], new_value: List[T] | None) -> None:
        self.table: List[str] = table
        self.column: List[str] = column
        self.conditions: List[Condition] = conditions
        self.new_value: List[T] | None = new_value
    
class DataDeletion:
    def __init__(self, table: str, conditions: List[Condition]) -> None:
        self.table: str = table
        self.conditions: List[Condition] = conditions

class Statistic:
    def __init__(self, n_r: int, l_r: int, f_r: int, V_a_r: Dict[str, int]) -> None:
        self.n_r: int = n_r
        self.l_r: int = l_r
        self.f_r: int = f_r
        self.b_r: int = ceil(n_r / f_r)
        self.V_a_r: Dict[str, int] = V_a_r