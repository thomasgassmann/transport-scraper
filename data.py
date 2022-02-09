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
    start_time_offset: time
    recurrence: int

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
