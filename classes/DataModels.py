from typing import Any, List, Generic, TypeVar, Dict
from enum import Enum
from math import ceil
from classes.Types import DataType, IntType, FloatType, CharType, VarCharType

T = TypeVar("T")

class Schema:
    """
        Schema.columns nyimpan gini:
        "id" = IntType()
        "name" = VarCharType(50)
        "ipk" = FloatType()
    """
    def __init__(self, **columns: DataType) -> None:
        self.columns = columns
        self.column_order = list(columns.keys())
        self.column_index = {name: i for i, name in enumerate(self.column_order)}
        self.size = self.calculate_row_size()

    def validate_tuple(self, values: Any) -> None:
        if len(values) != len(self.columns):
            raise ValueError("Value count doesn't match schema")
        for dtype, value in zip(self.columns.values(), values):
            dtype.validate(value)

    def calculate_row_size(self) -> int:
        size : int = 0
        for dtype in self.columns.values():
            if isinstance(dtype, IntType):
                size += 4
            elif isinstance(dtype, FloatType):
                size += 4
            elif isinstance(dtype, CharType):
                size += dtype.length
            elif isinstance(dtype, VarCharType):
                size += dtype.max_length
        return size

class Tuple:
    __slots__ = ['schema', 'values']
    def __init__(self, schema: Schema, *values: Any) -> None:
        if len(values) != len(schema.columns):
            raise ValueError("Value count doesn't match schema")
        
        for dtype, value in zip(schema.columns.values(), values):
            dtype.validate(value)

        self.schema = schema
        self.values = list(values)

    def __getitem__(self, column: str) -> Any:
        index = self.schema.column_index[column]
        return self.values[index]

    def __repr__(self) -> str:
        pairs = ", ".join(f"{col}={self.values[i]}" for i, col in enumerate(self.schema.column_order))
        return f"<Tuple {pairs}>"

class Operation(Enum):
    EQ = "="
    NEQ = "<>"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="

class Condition:
    def __init__(self, column: str, operation: Operation, operand: int | str | float) -> None:
        self.column: str = column
        self.operation: Operation = operation
        self.operand: int | str | float = operand

class DataRetrieval:
    def __init__(self, table: str, column: List[str], conditions: List[Condition]) -> None:
        self.table: str = table
        self.column: List[str] = column
        self.conditions: List[Condition] = conditions

class DataWrite(Generic[T]):
    def __init__(self, table: str, column: List[str], conditions: List[Condition], new_value: List[T] | None) -> None:
        self.table: str = table
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