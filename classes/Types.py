from typing import Any, List, Tuple

class DataType:
    def validate(self, value: Any) -> None:
        raise NotImplementedError()

class IntType(DataType):
    def validate(self, value: Any) -> None:
        if not isinstance(value, int):
            raise TypeError(f"Expected INT, got {type(value).__name__}")

    def to_dict(self) -> dict:
        return {"type": "int", "length": 4}

class FloatType(DataType):
    def validate(self, value) -> None:
        if not isinstance(value, (float, int)): 
            raise TypeError(f"Expected FLOAT, got {type(value).__name__}")

    def to_dict(self) -> dict:
        return {"type": "float", "length": 4}


class CharType(DataType):
    def __init__(self, length: int) -> None:
        self.length = length

    def validate(self, value) -> None:
        if not isinstance(value, str):
            raise TypeError(f"Expected CHAR({self.length}), got {type(value).__name__}")
        if len(value) != self.length:
            raise ValueError(f"CHAR({self.length}) requires exactly {self.length} characters")
    
    def to_dict(self) -> dict:
        return {"type": "char", "length": self.length}

class VarCharType(DataType):
    def __init__(self, max_length: int) -> None:
        self.max_length = max_length

    def validate(self, value) -> None:
        if not isinstance(value, str):
            raise TypeError(f"Expected VARCHAR({self.max_length}), got {type(value).__name__}")
        if len(value) > self.max_length:
            raise ValueError(f"VARCHAR({self.max_length}) allows up to {self.max_length} characters")
        
    def to_dict(self) -> dict:
        return {"type": "varchar", "length": self.max_length}