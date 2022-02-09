import random
import requests
import math
import os
import logging
import pickle
import datetime
import csv
import json
import zipfile
from datetime import datetime, time, timedelta
from typing import List
from io import TextIOWrapper
from dataclasses import dataclass, is_dataclass, asdict

# constants
MSSQL_OUT = 'init_mssql.sql'
MYSQL_OUT = 'init_mysql.sql'

NUM_CUSTOMERS = 5000
NUM_EMPLOYEES = 500
NUM_TICKETS = 100
EUCL_COST_FACTOR = 1000
NUM_AIRPORT_ROUTES = 50
AIRPORT_CLUSTER_DIST = 0.05

# init data structures

logging.basicConfig(level = logging.INFO)

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

customers = []
employees = []
stations = []
connections = []
tickets = []

# download and prepare data
if not os.path.isfile('cust.json'):
    res = requests.get(f'https://randomuser.me/api?results={NUM_CUSTOMERS}')
    res = res.json()['results']
    open('cust.json', 'w').write(json.dumps(res))
else:
    res = json.loads(open('cust.json', 'r').read())
customer_id = 1
for cust in res:
    customers.append(Customer(customer_id, cust['name']['first'], cust['name']['last']))
    customer_id += 1

if not os.path.isfile('emp.json'):
    res = requests.get(f'https://randomuser.me/api?results={NUM_EMPLOYEES}')
    res = res.json()['results']
    open('emp.json', 'w').write(json.dumps(res))
else:
    res = json.loads(open('emp.json', 'r').read())
seen_usernames = set()
for emp in res:
    username = emp['login']['username']
    # usernames need to be unique
    if username in seen_usernames:
        continue

    seen_usernames.add(username)
    employees.append(Employee(username, emp['login']['password']))

logging.info(f'Got {len(employees)} employeees')
logging.info(f'Got {len(customers)} customers')

GTFS_ZIP = 'gtfs.zip'
if not os.path.isfile(GTFS_ZIP):
    res = requests.get('https://opentransportdata.swiss/dataset/00811070-1b51-43da-87af-b1901e906323/resource/8fd78d4f-b63e-4f00-88a1-427207a7cc46/download/gtfs_fp2022_2022-02-02_17-26.zip')
    with open(GTFS_ZIP, 'wb') as f: 
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

logging.info('Got gtfs file')

BUS = 0b001
TRAIN = 0b010
PLANE = 0b100

airport_ids = []

def set_transport_type(station: Station) -> str:
    station.transport_type = BUS
    if 'Bahnhof' in station.name:
        station.transport_type |= TRAIN
    if 'Airport' in station.name or 'Flughafen' in station.name:
        if random.choice(['train', 'not_train']) == 'train':
            station.transport_type |= TRAIN
        station.transport_type |=  PLANE
        airport_ids.append(station.id)
    
def get_stop_id(val: str):
    if val.isdigit():
        return int(val)
    if ':' in val:
        idx = val.find(':')
        if val[0:idx].isdigit():
            return int(val[0:idx])
    return None

def parse_timestamp(ts: str):
    formats = [
        "%H:%M:%S", #HH:MM:SS
        "%H:%M:%S.%f", #HH:MM:SS.mm
        "%M:%S", #MM:SS
        "%M:%S.%f" #MM:SS.mm
    ]
    is_next_day = False
    # Thank you SBB!
    if ts[0:2].isdigit() and int(ts[0:2]) > 23:
        is_next_day = True
        next_day_offset = int(ts[0:2]) - 24
        ts = f'{next_day_offset:02d}:' + ts[3:]

    for f in formats:
        try:
            parsed =  datetime.strptime(ts, f)
            if is_next_day:
                parsed += timedelta(days=1)
            return parsed
        except ValueError:
            pass
    return None

archive = zipfile.ZipFile(GTFS_ZIP)
stops = archive.open('stops.txt')
locations = dict()
stations_by_id = dict()
seen_stations = set()
_ = stops.readline() # header
while (stop := stops.readline()):
    stopline = stop.decode()
    r = csv.reader([stopline])
    values = list(r)[0]
    stop_id = get_stop_id(values[0])
    if stop_id is not None and stop_id not in seen_stations:
        seen_stations.add(stop_id)
        station = Station(stop_id, values[1], 123)
        locations[station.id] = (float(values[2]), float(values[3]))
        set_transport_type(station)
        stations.append(station)

        stations_by_id[station.id] = station

logging.info(f'Read {len(stations)} stations')

stop_times = archive.open('stop_times.txt')
_ = stop_times.readline() # header
trips = dict()
while (stop_time := stop_times.readline()):
    stop_time_line = stop_time.decode()
    row = list(csv.reader([stop_time_line]))[0]
    trip_id = row[0]
    departure_time = row[1]
    stop_id = get_stop_id(row[3])
    seq = int(row[4])

    if trip_id not in trips:
        trips[trip_id] = []
    trips[trip_id].append((seq, stop_id, departure_time))

logging.info(f'Found {len(trips)} trips. Parsing connections...')

def eucl_dist(from_id, to_id):
    loc_from = locations[from_id]
    loc_to = locations[to_id]
    return (((loc_to[1] - loc_from[1]) ** 2) + ((loc_to[0] - loc_from[0]) ** 2)) ** 0.5

def eucl_dist_cost(from_id, to_id):
    return round(eucl_dist(from_id, to_id) * EUCL_COST_FACTOR, 2)

def set_cost(connection: Connection):
    cost = 0
    if connection.transport_type == PLANE:
        cost += random.randint(100, 140)

    if connection.transport_type == TRAIN:
        cost += random.randint(5, 10)
    
    cost += random.randint(0, 5)

    dist = eucl_dist_cost(connection.from_station_id, connection.to_station_id)

    connection.cost = cost + dist


connection_id = 1
seen_connections = set()
in_degrees = dict()
out_degrees = dict()

for station in stations:
    in_degrees[station.id] = 0
    out_degrees[station.id] = 0

for trip_id in trips:
    recs: List = trips[trip_id]
    recs.sort(key=lambda x: x[0])
    transport_type = random.choice([TRAIN, BUS])
    if transport_type == TRAIN and any(map(lambda x: stations_by_id[x[1]].transport_type & transport_type == 0, recs)):
        transport_type = BUS

    initial = parse_timestamp(recs[0][2])
    end_time = parse_timestamp(recs[len(recs) - 1][2])

    recurrence = int(math.ceil((end_time - initial).total_seconds() / 60)) * 2
    for i in range(len(recs) - 1):
        from_rec = recs[i]
        to_rec = recs[i + 1]

        from_station = from_rec[1]
        to_station = to_rec[1]

        key_tuple = (from_station, to_station)
        if to_station not in seen_stations or from_station not in seen_stations or key_tuple in seen_connections:
            continue

        arrival: datetime = parse_timestamp(to_rec[2])
        departure: datetime = parse_timestamp(from_rec[2])
        duration = int(math.ceil((arrival - departure).total_seconds() / 60))

        connec = Connection(connection_id, from_station, to_station, transport_type, duration, 0, initial.time(), recurrence)
        set_cost(connec)
        connections.append(connec)
        connection_id += 1
        seen_connections.add(key_tuple)
        in_degrees[to_station] += 1
        out_degrees[from_station] += 1

# generate some airports routes
# first, merge airports and select a representative

def cluster_airport():
    airports = airport_ids
    C = []
    while len(airports):
        locus = airports.pop()
        cluster = [x for x in airports if eucl_dist(locus, x) <= AIRPORT_CLUSTER_DIST]
        C.append(cluster + [locus])
        for x in cluster:
            airports.remove(x)
    return C

representatives = []
items = dict()
for group in cluster_airport():
    group.sort(key=lambda x: len(stations_by_id[x].name))
    representative = group.pop()
    representatives.append(representative)
    empty_time = datetime(2000, 1, 1, 0, 0, 0).time()
    items[representative] = []
    for item in group:
        items[representative].append(item)

        forth = Connection(connection_id, representative, item, BUS, 0, 0, empty_time, 0)
        connection_id += 1

        back = Connection(connection_id, item, representative, BUS, 0, 0, empty_time, 0)
        connection_id += 1

        connections.append(forth)
        connections.append(back)

for i in range(NUM_AIRPORT_ROUTES):
    from_airport = representatives[random.randint(0, len(representatives) - 1)]
    connecting_airport = [repre for repre in representatives if repre != from_airport]
    connecting_airport.sort(key=lambda x: random.randint(1, 10))
    connecting_airport = connecting_airport[0]
    cost = eucl_dist(from_airport, connecting_airport) * AIRPORT_CLUSTER_DIST + random.randint(50, 150)
    duration = eucl_dist(from_airport, connecting_airport) * AIRPORT_CLUSTER_DIST * 10

    time = datetime(2000, 1, 1, random.randint(0, 23), random.randint(0, 59), 0).time()

    connections.append(Connection(connection_id, from_airport, connecting_airport, PLANE, duration, round(cost, 2), time, 60 * 12))
    connection_id += 1

logging.info(f'Found {len(connections)} connections in routes.')

with open('deg.json', 'w') as f:
    json.dump({
        'in': in_degrees,
        'out': out_degrees
    }, f)


ticket_id = 1
for i in range(NUM_TICKETS):
    tickets.append(Ticket(
        ticket_id,
        connections[random.randint(0, len(connections) - 1)].id,
        customers[random.randint(0, len(customers) - 1)].id,
        random.choice([True, False])))
    ticket_id += 1

logging.info(f'Generated {len(tickets)} tickets on some routes')

# dump
with open('data.pickle', 'wb') as f:
    pickle.dump({
        'customers': customers,
        'employees': employees,
        'stations': stations,
        'station_locations': locations,
        'tickets': tickets,
        'connections': connections
    }, f)

# create sql files
def write_tickets(sql: TextIOWrapper):
    sql.writelines([
        '''create table Ticket(
                Id integer primary key,
                ConnectionId integer foreign key not null references Connection(Id),
                CustomerId integer foreign key not null references Customer(Id),
                OneWay bit not null);\n\n'''
    ])

    for ticket in tickets:
        sql.writelines([
            f'insert into Ticket(Id, ConnectionId, CustomerId, OneWay) values ({ticket.id}, {ticket.connection_id}, {ticket.customer_id}, {1 if ticket.one_way else 0});\n'
        ])

def write_connections(sql: TextIOWrapper):
    sql.writelines([
        '''create table Connection(
                Id integer primary key,
                FromStationId integer foreign key not null references Station(Id),
                ToStationId integer foreign key not null references Station(Id),
                TransportType int not null,
                Duration int not null,
                Cost int not null,
                StartTimeOffset time not null
                Recurrence int not null);\n\n'''
    ])

    for connection in connections:
        sql.writelines([
            f'''insert into Connection(Id, FromStationId, ToStationId, TransportType, Duration, Cost, StartTimeOffset, Recurrence) values 
                ({connection.id}, {connection.from_station_id}, {connection.to_station_id}, {connection.transport_type}, {connection.duration}, {connection.cost}, {connection.start_time_offset}, {connection.recurrence});\n'''
        ])

def write_stations(sql: TextIOWrapper):
    sql.writelines([
        '''create table Station(
                Id integer primary key,
                Name varchar(100) not null,
                TransportType int not null);\n\n'''
    ])

    for station in stations:
        sql.writelines([
            f'insert into Station(Id, Name, TransportType) values ({station.id}, \'{station.name}\', {station.transport_type});\n'
        ])
    

def write_employees(sql: TextIOWrapper):
    sql.writelines([
        '''create table Employee(
                Username varchar(100) primary key,
                [Password] varchar(100) not null);\n\n'''
    ])

    for employee in employees:
        sql.writelines([
            f'insert into Employee(Username, [Password]) values (\'{employee.user_name}\', \'{employee.password}\');\n'
        ])


def write_customers(sql: TextIOWrapper):
    sql.writelines([
        '''create table Customer(
                Id integer not null primary key,
                FirstName varchar(100) not null,
                LastName varchar(100) not null);\n\n'''
    ])

    for customer in customers:
        sql.writelines([
            f'insert into Customer(Id, FirstName, LastName) values ({customer.id}, \'{customer.first_name}\', \'{customer.last_name}\');\n'
        ])

with open(MSSQL_OUT, 'w') as mssql:
    with open(MYSQL_OUT, 'w') as mysql:
        write_customers(mssql)
        write_customers(mysql)

        mssql.write('\n\n')
        mysql.write('\n\n')

        write_employees(mssql)
        write_employees(mysql)

        mssql.write('\n\n')
        mysql.write('\n\n')

        write_stations(mssql)
        write_stations(mysql)

        mssql.write('\n\n')
        mysql.write('\n\n')

        write_connections(mssql)
        write_connections(mysql)

        mssql.write('\n\n')
        mysql.write('\n\n')

        write_tickets(mssql)
        write_tickets(mysql)
