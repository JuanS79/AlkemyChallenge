from pathlib import Path
from  sqlalchemy import create_engine
from  sqlalchemy import text
import psycopg2

sql= Path("sql", "tabla.sql")
print(sql)
engine= create_engine("postgresql+psycopg2://postgres:Fullsite1*@localhost:5432/museos")
with open (sql) as f:
    query = text(f.read())
    print(query)
    engine.execute("DROP TABLE IF EXISTS tabla_prueba")
    engine.execute(query)


