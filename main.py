import requests
from io import TextIOWrapper
from dataclasses import dataclass

MSSQL_OUT = 'init_mssql.sql'
MYSQL_OUT = 'init_mysql.sql'

NUM_CUSTOMERS = 5000

@dataclass
class Customer:
    first_name: str
    last_name: str

customers = []

res = requests.get(f'https://randomuser.me/api?results={NUM_CUSTOMERS}')
for cust in res.json()['results']:
    customers.append(Customer(cust['name']['first'], cust['name']['last']))

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
            f'insert into Customer(Id, FirstName, LastName) values ({customer_id}, \'{customer.first_name}\', \'{customer.last_name}\')\n'
        ])
        customer_id += 1

with open(MSSQL_OUT, 'w') as mssql:
    with open(MYSQL_OUT, 'w') as mysql:
        write_customers(mssql)
        write_customers(mysql)
