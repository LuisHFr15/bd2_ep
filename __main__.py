import sqlite3
from utils.db import create_database
from faker import Faker
from utils.db import alimenta_db_pessoa
import datetime, time

con = sqlite3.connect('./.storage/database.db')

cursor = con.cursor()
cursor.execute("PRAGMA foreign_key = ON;")
# use placeholder ? instead of using python formating to avoid sql injection
con.close()
create_database()
worked = alimenta_db_pessoa(1000000)