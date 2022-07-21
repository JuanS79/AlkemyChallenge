from base64 import encode
from calendar import month
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd

from sqlalchemy import create_engine

from decouple import config

#----------------------------------------------------------------------------------------------------
# Extracción de datos de Museos

url= "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv"
museos=requests.get(url)

file_path_crib= '{category}/{year}-{month}/'
nombre= "{category}-{day}-{month}-{year}.csv"

now=datetime.now()
directorio_museos= file_path_crib.format(category="museos", year=now.year, month=now.month)
museo_txt=nombre.format(category="museos", year=now.year, month=now.month, day=now.day)

directorio_base= Path("d:/alkemy2.0")
museo_directorio= Path(directorio_base, directorio_museos, museo_txt)
museo_directorio.parent.mkdir(parents=True, exist_ok=True)

with open(museo_directorio, "w", encoding="ISO-8859-1" ) as museo:
    museo.write(museos.text)


#----------------------------------------------------------------------------------------------------
# Creacion del Dataframe de Museos

df = pd.read_csv(museo_directorio)

renamed_cols = {
    'Cod_Loc':'cod_localidad',
    'IdProvincia':'id_provincia',
    'IdDepartamento' : 'id_departamento',
    'categoria' : 'categoría',
    'direccion' : 'domicilio',
    'CP' : 'codigo postal',
    'telefono': 'número de teléfono',
    'Mail': 'mail',
    'Web': 'web',
}

df = df.rename(columns=renamed_cols)

colum_list = [
    'cod_localidad',
    'id_provincia',
    'id_departamento',
    'categoría',
    'provincia',
    'localidad',
    'nombre',
    'domicilio',
    'codigo postal',
    'número de teléfono',
    'mail',
    'web', 
]

df_museos= df[colum_list]


#----------------------------------------------------------------------------------------------------
# Extracción de datos de Salas de Cines

url_cines= "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv"
cines=requests.get(url_cines)

file_path_crib= '{category}/{year}-{month}/'
nombre= "{category}-{day}-{month}-{year}.csv"

now=datetime.now()
directorio_cines= file_path_crib.format(category="cines", year=now.year, month=now.month)
cines_txt=nombre.format(category="cines", year=now.year, month=now.month, day=now.day)

directorio_base= Path("d:/alkemy2.0")
cines_directorio= Path(directorio_base, directorio_cines, cines_txt)
cines_directorio.parent.mkdir(parents=True, exist_ok=True)

with open(cines_directorio, "w", encoding="ISO-8859-1" ) as cine:
    cine.write(cines.text)

#----------------------------------------------------------------------------------------------------
# Creacion del Dataframe de Salas de Cines

df_cines = pd.read_csv(cines_directorio)

renamed_cols = {
    'Cod_Loc':'cod_localidad',
    'IdProvincia':'id_provincia',
    'IdDepartamento' : 'id_departamento',
    'Categoría' : 'categoría',
    'Provincia' : 'provincia',
    'Localidad' : 'localidad',
    'Nombre' : 'nombre',
    'Dirección' : 'domicilio',
    'CP' : 'codigo postal',
    'Teléfono': 'número de teléfono',
    'Mail': 'mail',
    'Web': 'web',
    'Pantallas': 'pantallas', 
    'Butacas':'butacas', 
}

df_cines = df_cines.rename(columns=renamed_cols)


colum_list = [
    'cod_localidad',
    'id_provincia',
    'id_departamento',
    'categoría',
    'provincia',
    'localidad',
    'nombre',
    'domicilio',
    'codigo postal',
    'número de teléfono',
    'mail',
    'web',
    'pantallas', 
    'butacas', 
    'espacio_INCAA', 
]

df_cines= df_cines[colum_list]


#----------------------------------------------------------------------------------------------------
# Extracción de datos de Salas de Bibliotecas

url_bibliotecas= "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"
bibliotecas=requests.get(url_bibliotecas)

file_path_crib= '{category}/{year}-{month}/'
nombre= "{category}-{day}-{month}-{year}.csv"

now=datetime.now()
directorio_bibliotecas= file_path_crib.format(category="bibliotecas", year=now.year, month=now.month)
bibliotecas_txt=nombre.format(category="bibliotecas", year=now.year, month=now.month, day=now.day)

directorio_base= Path("d:/alkemy2.0")
bibliotecas_directorio= Path(directorio_base, directorio_bibliotecas, bibliotecas_txt)
bibliotecas_directorio.parent.mkdir(parents=True, exist_ok=True)

with open(bibliotecas_directorio, "w", encoding="ISO-8859-1" ) as biblioteca:
    biblioteca.write(bibliotecas.text)

#----------------------------------------------------------------------------------------------------
# Creacion del Dataframe de Salas de bibliotecas

df_bibliotecas = pd.read_csv(bibliotecas_directorio)

renamed_cols = {
    'Cod_Loc':'cod_localidad',
    'IdProvincia':'id_provincia',
    'IdDepartamento' : 'id_departamento',
    'Categoría' : 'categoría',
    'Provincia' : 'provincia',
    'Localidad' : 'localidad',
    'Nombre' : 'nombre',
    'Domicilio' : 'domicilio',
    'CP' : 'codigo postal',
    'Teléfono': 'número de teléfono',
    'Mail': 'mail',
    'Web': 'web',
}

df_bibliotecas = df_bibliotecas.rename(columns=renamed_cols)


colum_list = [
    'cod_localidad',
    'id_provincia',
    'id_departamento',
    'categoría',
    'provincia',
    'localidad',
    'nombre',
    'domicilio',
    'codigo postal',
    'número de teléfono',
    'mail',
    'web', 
]

df_bibliotecas= df_bibliotecas[colum_list]


#Variables de la base de Datos

user=config('USER')
password=config('PASSWORD')
data_base=config('DATABASE')

#Paso del Dataframe a la base de datos.

engine= create_engine(f"postgresql://{user}:{password}@localhost:5432/{data_base}")

df_museos.to_sql("museos_tabla", con = engine,if_exists="replace")
df_cines.to_sql("cines_tabla", con = engine ,if_exists="replace")
df_bibliotecas.to_sql("bibliotecas_tabla", con = engine, if_exists="replace")

#Juntamos todos los datos en un mismo Dataframe

dfss = pd.concat([df_museos,df_cines,df_bibliotecas])
dfss.to_sql("datos_todo_junto", con = engine, if_exists="replace")


#Juntamos todos los datos por Categoría

df_por_categoría= dfss.groupby('categoría', as_index=False).size()
df_por_categoría.to_sql("datos_por_categoría", con = engine, if_exists="replace")

#Juntamos todos los datos por Provincia y Categoría

df_por_provincia_categoría= dfss.groupby(['categoría', 'provincia'], as_index=False).size()
df_por_provincia_categoría.to_sql("datos_por_provincia_categoría", con = engine, if_exists="replace")

#Juntamos todos los datos por Provincia, Cantidad de pantallas, Cantidad de butacas y Cantidad de espacios INCAA

df_cines_prov_pant_but_esp= df_cines.groupby('provincia', as_index=False).count()[['provincia', 'pantallas','butacas','espacio_INCAA']]
df_cines_prov_pant_but_esp.to_sql("cines_prov_pant_but_esp", con = engine, if_exists="replace")

