import requests
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
