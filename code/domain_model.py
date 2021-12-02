from dataclasses import dataclass, field
from typing import List


@dataclass
class Industry:
    id: int
    name: str
    source: str
    value: str


@dataclass
class Security:
    id: int
    name: str
    description: str
    cusip: str
    industries: List[Industry] = field(default_factory=list)


@dataclass
class Position:
    market_value: float
    exposure: str
    account_id: int
    security_id: int


@dataclass
class Account:
    id: int
    name: str
    description: str
    positions: List[Position] = field(default_factory=list)
