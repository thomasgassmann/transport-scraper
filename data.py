from dataclasses import dataclass
from datetime import time

@dataclass
class Customer:
    id: int
    first_name: str
    last_name: str

@dataclass
class Station:
    id: int
    name: str
    transport_type: int

@dataclass
class Connection:
    id: int
    from_station_id: int
    to_station_id: int
    transport_type: int
    duration: int
    cost: float

@dataclass
class Ticket:
    id: int
    connection_id: int
    customer_id: int
    one_way: bool

@dataclass
class Employee:
    user_name: str
    password: str
    counter_station_id: int

BUS = 0b001
TRAIN = 0b010
PLANE = 0b100