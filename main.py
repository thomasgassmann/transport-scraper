import requests
import os
import csv
import zipfile
import json
from io import TextIOWrapper
from dataclasses import dataclass, is_dataclass, asdict

# constants
MSSQL_OUT = 'init_mssql.sql'
MYSQL_OUT = 'init_mysql.sql'

NUM_CUSTOMERS = 5000
NUM_EMPLOYEES = 500

# init data structures

@dataclass
class Customer:
    id: int
    first_name: str
    last_name: str

@dataclass
class Station:
    id: int
    name: str
    station_type: int

@dataclass
class Connection:
    pass

@dataclass
class Ticket:
    pass

@dataclass
class Employee:
    user_name: str
    password: str

customers = []
employees = []
stations = []

# download and prepare data

res = requests.get(f'https://randomuser.me/api?results={NUM_CUSTOMERS}')
customer_id = 1
for cust in res.json()['results']:
    customers.append(Customer(customer_id, cust['name']['first'], cust['name']['last']))
    customer_id += 1

res = requests.get(f'https://randomuser.me/api?results={NUM_EMPLOYEES}')
seen_usernames = set()
for emp in res.json()['results']:
    username = emp['login']['username']
    # usernames need to be unique
    if username in seen_usernames:
        continue

    seen_usernames.add(username)
    employees.append(Employee(username, emp['login']['password']))

GTFS_ZIP = 'gtfs.zip'
if not os.path.isfile(GTFS_ZIP):
    res = requests.get('https://opentransportdata.swiss/dataset/00811070-1b51-43da-87af-b1901e906323/resource/8fd78d4f-b63e-4f00-88a1-427207a7cc46/download/gtfs_fp2022_2022-02-02_17-26.zip')
    with open(GTFS_ZIP, 'wb') as f: 
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

archive = zipfile.ZipFile(GTFS_ZIP)
stops = archive.open('stops.txt')
_ = stops.readline() # header
while (stop := stops.readline()):
    stopline = stop.decode()
    r = csv.reader([stopline])
    values = list(r)[0]
    if values[0].isdigit():
        # TODO:
        stations.append(Station(int(values[0]), values[1], 123))


# dump
class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if is_dataclass(o):
                return asdict(o)
            return super().default(o)

with open('data.json', 'w') as f:
    f.write(json.dumps({
        'customers': customers,
        'employees': employees,
        'stations': stations
    }, cls=EnhancedJSONEncoder, indent=2))

# create sql files

def write_stations(sql: TextIOWrapper):
    sql.writelines([
        '''create table Station(
                Id integer primary key,
                Name varchar(100) not null,
                TransportType int not null);\n\n'''
    ])

    for station in stations:
        sql.writelines([
            f'insert into Station(Id, Name, TransportType) values ({station.id}, \'{station.name}\', {station.station_type});\n'
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
