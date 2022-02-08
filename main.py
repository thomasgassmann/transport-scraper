import requests
import os
from io import TextIOWrapper
from dataclasses import dataclass

MSSQL_OUT = 'init_mssql.sql'
MYSQL_OUT = 'init_mysql.sql'

NUM_CUSTOMERS = 5000
NUM_EMPLOYEES = 500

@dataclass
class Customer:
    first_name: str
    last_name: str

@dataclass
class Station:
    name: str
    station_type: int

@dataclass
class Employee:
    user_name: str
    password: str

customers = []
employees = []

res = requests.get(f'https://randomuser.me/api?results={NUM_CUSTOMERS}')
for cust in res.json()['results']:
    customers.append(Customer(cust['name']['first'], cust['name']['last']))

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
    res = requests.get('https://opentransportdata.swiss/dataset/1aff176a-9665-4395-a3b1-03e3032a0373/resource/f587c0fb-e410-4fd2-a468-4f6c4c40a049/download/gtfs_fp2021_2021-12-08_09-10.zip')
    with open(GTFS_ZIP, 'wb') as f: 
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


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
    customer_id = 1
    sql.writelines([
        '''create table Customer(
                Id integer not null primary key,
                FirstName varchar(100) not null,
                LastName varchar(100) not null);\n\n'''
    ])

    for customer in customers:
        sql.writelines([
            f'insert into Customer(Id, FirstName, LastName) values ({customer_id}, \'{customer.first_name}\', \'{customer.last_name}\');\n'
        ])
        customer_id += 1

with open(MSSQL_OUT, 'w') as mssql:
    with open(MYSQL_OUT, 'w') as mysql:
        write_customers(mssql)
        write_customers(mysql)

        mssql.write('\n\n')
        mysql.write('\n\n')

        write_employees(mssql)
        write_employees(mysql)
