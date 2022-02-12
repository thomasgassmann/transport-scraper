from itertools import groupby
import random
from types import NoneType
import requests
import math
import os
import logging
import pickle
import datetime
import csv
import json
import zipfile
import statistics
from datetime import datetime, timedelta
from typing import List
from io import TextIOWrapper
from bellman_ford import bellman_ford
from data import Connection, Customer, Employee, Station, Ticket, BUS, TRAIN, PLANE

# constants
MSSQL_OUT = 'init_mssql.sql'
MYSQL_OUT = 'init_mysql.sql'

NUM_CUSTOMERS = 500
NUM_EMPLOYEES = 100
NUM_TICKETS = 100
EUCL_COST_FACTOR = 1000
NUM_AIRPORT_ROUTES = 50
AIRPORT_CLUSTER_DIST = 0.05
STATION_CLUSTER_DIST = 0.03
REACHABILITY_FROM_REQUIRED = [8503000, 8503016, 8583259, 8592929, 8572991, 8502004, 8588465, 8500682]
MAX_STATIONS = 10000
MAX_CONNECTIONS = 50000
NO_PRUNE = False

# init data structures

logging.basicConfig(level = logging.INFO)

customers = []
employees = []
stations = []
connections = []
tickets = []

# download and prepare data
if not os.path.isfile('cust.json'):
    res = requests.get(f'https://randomuser.me/api?results={NUM_CUSTOMERS}&nat=ch')
    res = res.json()['results']
    open('cust.json', 'w').write(json.dumps(res))
else:
    res = json.loads(open('cust.json', 'r').read())
customer_id = 1
for cust in res:
    customers.append(Customer(customer_id, cust['name']['first'], cust['name']['last']))
    customer_id += 1

if not os.path.isfile('emp.json'):
    res = requests.get(f'https://randomuser.me/api?results={NUM_EMPLOYEES}&nat=ch')
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
    employees.append(Employee(username, emp['login']['password'], -1))

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

airport_ids = []

def set_transport_type(station: Station) -> str:
    station.transport_type = BUS
    if 'Bahnhof' in station.name or 'HB' in station.name:
        station.transport_type |= TRAIN
    if 'Airport' in station.name or 'Flughafen' in station.name or 'Aéroport' in station.name:
        if random.choice(['train', 'not_train']) == 'train':
            station.transport_type |= TRAIN
        station.transport_type |= PLANE
        airport_ids.append(station.id)
    else:
        station.transport_type |= random.choice([TRAIN, BUS, BUS])
    
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
    if (stations_by_id[recs[0][1]].transport_type & TRAIN == TRAIN and stations_by_id[recs[len(recs) - 1][1]].transport_type & TRAIN == TRAIN) or \
        all(map(lambda x: stations_by_id[x[1]].transport_type & TRAIN == TRAIN, recs)):
        transport_type = TRAIN
    else:
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
        duration = int(math.ceil((arrival - departure).total_seconds() / 60)) + 5

        if stations_by_id[from_station].transport_type & transport_type == 0:
            stations_by_id[from_station].transport_type |= transport_type
        if stations_by_id[to_station].transport_type & transport_type == 0:
            stations_by_id[to_station].transport_type |= transport_type

        connec = Connection(connection_id, from_station, to_station, transport_type, duration, 0)
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
    if any([x in REACHABILITY_FROM_REQUIRED for x in group]):
        group.sort(key=lambda x: x in REACHABILITY_FROM_REQUIRED, reverse=True)
    else:
        group.sort(key=lambda x: len(stations_by_id[x].name))
    representative = group.pop()
    representatives.append(representative)
    items[representative] = []
    for item in group:
        items[representative].append(item)

        forth = Connection(connection_id, representative, item, BUS, 1, 1)
        connection_id += 1

        back = Connection(connection_id, item, representative, BUS, 1, 1)
        connection_id += 1

        connections.append(forth)
        connections.append(back)

for i in range(NUM_AIRPORT_ROUTES):
    from_airport = representatives[random.randint(0, len(representatives) - 1)]
    connecting_airport = [repre for repre in representatives if repre != from_airport]
    connecting_airport.sort(key=lambda x: random.randint(1, 10))
    connecting_airport = connecting_airport[0]
    cost = eucl_dist(from_airport, connecting_airport) * AIRPORT_CLUSTER_DIST + random.randint(50, 150)
    duration = eucl_dist(from_airport, connecting_airport) * AIRPORT_CLUSTER_DIST * 500 + 5

    time = datetime(2000, 1, 1, random.randint(0, 23), random.randint(0, 59), 0).time()

    connections.append(Connection(connection_id, from_airport, connecting_airport, PLANE, duration, round(cost, 2)))
    connection_id += 1

logging.info(f'Found {len(connections)} connections in routes.')

if not NO_PRUNE:
    for item in REACHABILITY_FROM_REQUIRED:
        logging.info(f'Pruning connections not reachable from {stations_by_id[item].name}...')

    removed_stations = set()

    def remove_station(id):
        station = stations_by_id[id]
        stations.remove(station)
        removed_stations.add(station.id)

    def prune_connections():
        for connection in connections:
            if connection.from_station_id in removed_stations or connection.to_station_id in removed_stations:
                connections.remove(connection)

    def remove_unreachable():
        for item in REACHABILITY_FROM_REQUIRED:
            (distances, parent, via, iterations) = bellman_ford(item, connections, lambda x: 1)
            for station in stations:
                if station.id not in distances:
                    remove_station(station.id)
            prune_connections()

    remove_unreachable()

    logging.info(f'{len(stations)} stations left. {len(connections)} connections left.')

    redirects = dict()

    def follow_redirect(src):
        if src not in redirects:
            return src
        return follow_redirect(redirects[src])

    def merge(src, tar):
        target_redir = follow_redirect(src.id)
        target_station = stations_by_id[target_redir]
        target_station.transport_type |= tar.transport_type
        if len(target_station.name) > len(tar.name) and target_station.transport_type & PLANE != PLANE and target_station.id not in REACHABILITY_FROM_REQUIRED:
            logging.info(f'Renaming {target_station.name} to {tar.name}')
            target_station.name = tar.name
        remove_station(tar.id)
        redirects[tar.id] = target_redir

    def cluster_stations():
        src_station = stations[random.randint(0, len(stations) - 1)]
        for station in stations:
            # we don't merge stations from which reachability is required
            if eucl_dist(src_station.id, station.id) < STATION_CLUSTER_DIST and station.id != src_station.id and station.id not in REACHABILITY_FROM_REQUIRED:
                logging.info(f'Merging {stations_by_id[src_station.id].name} and {stations_by_id[station.id].name}')
                merge(src_station, station)
                return

    logging.info('Clustering stations')
    while len(stations) > MAX_STATIONS:
        cluster_stations()
        logging.info(f'{len(stations)} stations left...')
    
    logging.info('Redirecting connections')
    for conn in connections:
        if conn.from_station_id in redirects or conn.to_station_id in redirects:
            if conn.from_station_id in redirects:
                conn.from_station_id = follow_redirect(conn.from_station_id)
            if conn.to_station_id in redirects:
                conn.to_station_id = follow_redirect(conn.to_station_id)
            logging.info(f'Redirected connection from {stations_by_id[conn.from_station_id].name} to {stations_by_id[conn.to_station_id].name}')

    logging.info(f'There are {len(connections)} connections. Removing duplicates between merged stations...')
    for key, group in groupby(connections, lambda x: (x.from_station_id, x.to_station_id)):
        g = list(group)
        g.sort(key=lambda x: x.transport_type != BUS)

        first = True
        for item in g:
            if first:
                try:
                    item.duration = int(statistics.mean(list(map(lambda x: x.duration, g)))) + 3
                    item.cost = statistics.mean(list(map(lambda x: x.cost, g)))
                except:
                    logging.info(f'Could not update pricing from {stations_by_id[item.from_station_id].name} to {stations_by_id[item.to_station_id].name}')
                first = False
                continue
            connections.remove(item)

    while len(connections) > MAX_CONNECTIONS:
        cur = connections[random.randint(0, len(connections) - 1)]
        if cur.transport_type & PLANE == PLANE or cur.transport_type & TRAIN == TRAIN:
            continue

        connections.remove(cur)
    
    remove_unreachable()

    valid_stations = set()
    for station in stations:
        valid_stations.add(station.id)
    to_remove = []
    for connection in connections:
        if connection.from_station_id not in valid_stations or connection.to_station_id not in valid_stations or (connection.from_station_id == connection.to_station_id):
            to_remove.append(connection)
            logging.info(f'Removing {stations_by_id[connection.from_station_id].name} - {stations_by_id[connection.to_station_id].name}')
    for item in to_remove:
        connections.remove(item)

    logging.info(f'{len(connections)} connections left...')

# assign stations to employees
for emp in employees:
    emp.counter_station_id = stations[random.randint(0, len(stations) - 1)].id

employees.append(Employee('admin', 'admin', 8503000)) # Zürich HB
stations.insert(random.randint(0, len(stations) - 1), Station(1, 'Regionalmeisterschaften', BUS))
stations.insert(random.randint(0, len(stations) - 1), Station(2, 'ICTSkills', BUS))
stations.insert(random.randint(0, len(stations) - 1), Station(3, 'WorldSkills', PLANE))
connections.insert(random.randint(0, len(connections) - 1), Connection(connection_id, 1, 2, TRAIN, 1000, 1))
connection_id += 1
connections.insert(random.randint(0, len(connections) - 1), Connection(connection_id, 2, 3, PLANE, 10000, 1))

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
def write_tables(sql: TextIOWrapper, mssql: bool):
    sql.writelines([
        '''create table Customer(
                Id integer not null,
                FirstName varchar(100) not null,
                LastName varchar(100) not null,
                constraint pk_customer_id primary key (Id));\n\n'''
    ])
    sql.writelines([
        '''create table Station(
                Id integer not null,
                Name varchar(100) not null,
                constraint pk_station_id primary key (Id));\n\n'''
    ])
    sql.writelines([
        '''create table StationConnection(
                Id integer not null,
                FromStationId integer not null,
                ToStationId integer not null,
                TransportType integer not null,
                Duration integer not null,
                Cost decimal(10, 2) not null,
                constraint pk_station_connection_id primary key (Id),
                constraint fk_from_station_id foreign key (FromStationId) references Station(Id),
                constraint fk_to_station_id foreign key (ToStationId) references Station(Id));\n\n'''
    ])
    sql.writelines([
        f'''create table Ticket(
                Id integer not null {'identity(1, 1)' if mssql else 'auto_increment'},
                StationConnectionId integer not null,
                CustomerId integer not null,
                OneWay bit not null,
                constraint pk_ticket_id primary key (Id),
                constraint fk_connection_id foreign key (StationConnectionId) references StationConnection(Id),
                constraint fk_customer_id foreign key (CustomerId) references Customer(Id));\n\n'''
    ])
    sql.writelines([
        '''create table Employee(
                Username varchar(100) not null,
                Password varchar(100) not null,
                CounterStationId integer not null,
                constraint pk_employee_username primary key (Username),
                constraint fk_counter_station_id foreign key (CounterStationId) references Station(Id));\n\n'''
    ])

def p(s: str):
    return s.replace('\'', '\'\'')

def t(s: int):
    if s & PLANE == PLANE:
        return 2
    if s & TRAIN == TRAIN:
        return 1
    return 0

def d(s: float):
    return f'{s:.2f}'

def insert_statements_mysql(sql: TextIOWrapper, header, l: List, transform):
    sql.writelines('\n')
    first = l.pop()
    sql.write(f'insert into {header} values {transform(first)}')
    for item in l:
        sql.write(f', {transform(item)}')
    sql.write(';\n')


def write_tickets_mysql(sql: TextIOWrapper):
    insert_statements_mysql(
        sql, 
        'Ticket(StationConnectionId, CustomerId, OneWay)', 
        tickets, 
        lambda x: f'({x.connection_id}, {x.customer_id}, {1 if x.one_way else 0})')

def write_connections_mysql(sql: TextIOWrapper):
    insert_statements_mysql(
        sql,
        'StationConnection(Id, FromStationId, ToStationId, TransportType, Duration, Cost)',
        connections,
        lambda connection: f'({connection.id}, {connection.from_station_id}, {connection.to_station_id}, {t(connection.transport_type)}, {connection.duration}, {d(connection.cost)})'
    )

def write_stations_mysql(sql: TextIOWrapper):
    insert_statements_mysql(
        sql,
        'Station(Id, Name)',
        stations,
        lambda station: f'({station.id}, \'{p(station.name)}\')'
    )

def write_employees_mysql(sql: TextIOWrapper):
    insert_statements_mysql(
        sql,
        'Employee(Username, Password, CounterStationId)',
        employees,
        lambda employee: f'(\'{p(employee.user_name)}\', \'{p(employee.password)}\', {employee.counter_station_id})'
    )

def write_customers_mysql(sql: TextIOWrapper):
    insert_statements_mysql(
        sql,
        'Customer(Id, FirstName, LastName)',
        customers,
        lambda customer: f'({customer.id}, \'{p(customer.first_name)}\', \'{p(customer.last_name)}\')'
    )

def write_tickets_mssql(sql: TextIOWrapper):
    sql.write('\n\n')
    for ticket in tickets:
        sql.writelines([
            f'insert into Ticket(StationConnectionId, CustomerId, OneWay) values ({ticket.connection_id}, {ticket.customer_id}, {1 if ticket.one_way else 0});\n'
        ])

def write_connections_mssql(sql: TextIOWrapper):
    sql.write('\n\n')
    for connection in connections:
        sql.writelines([
            f'''insert into StationConnection(Id, FromStationId, ToStationId, TransportType, Duration, Cost) values ({connection.id}, {connection.from_station_id}, {connection.to_station_id}, {t(connection.transport_type)}, {connection.duration}, {d(connection.cost)});\n'''
        ])

def write_stations_mssql(sql: TextIOWrapper):
    sql.write('\n\n')
    for station in stations:
        sql.writelines([
            f'insert into Station(Id, Name) values ({station.id}, \'{p(station.name)}\');\n'
        ])
    

def write_employees_mssql(sql: TextIOWrapper):
    sql.write('\n\n')
    for employee in employees:
        sql.writelines([
            f'insert into Employee(Username, Password, CounterStationId) values (\'{p(employee.user_name)}\', \'{p(employee.password)}\', {employee.counter_station_id});\n'
        ])


def write_customers_mssql(sql: TextIOWrapper):
    sql.write('\n\n')
    for customer in customers:
        sql.writelines([
            f'insert into Customer(Id, FirstName, LastName) values ({customer.id}, \'{p(customer.first_name)}\', \'{p(customer.last_name)}\');\n'
        ])

with open(MSSQL_OUT, 'w') as mssql:
    with open(MYSQL_OUT, 'w') as mysql:
        for item in [mssql, mysql]:
            write_tables(item, item == mssql)

            if item == mysql:
                write_customers_mysql(item)
                write_stations_mysql(item)
                write_connections_mysql(item)
                write_tickets_mysql(item)
                write_employees_mysql(item)
            else:
                write_customers_mssql(item)
                write_stations_mssql(item)
                write_connections_mssql(item)
                write_tickets_mssql(item)
                write_employees_mssql(item)