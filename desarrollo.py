#Se importan las librería que se van a utilizar

import requests    #para obtener los archivos fuente
import os    #proporciona funciones para interactuar con el sistema operativo
import pandas as pd    #para procesar todos los datos necesarios
from datetime import datetime    #proporciona clases para manipular fechas y horas
import logging    #para crear logs oportunos sobre la ejecución del programa
from sqlalchemy import create_engine    #para realizar la conexión a la base de datos
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.declarative import declarative_base
from decouple import Config    #ayuda a separar los parámetros de configuración del código fuente
import psycopg2    #adaptador de base de datos de Postgres

#Se emplea "logging" para notificar las excepciones que puedan ocurrir
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
    )

#Se obtienen las url de los archivos fuente de datos

museos = 'https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d'
cines = 'https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae'
bibliotecas = 'https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7'

#Se crean las variables día, mes, año y fecha con el mòdulo "datetime"
día = datetime.today().strftime('%d')
mes = datetime.today().strftime('%B')
año = datetime.today().strftime('%Y')
fecha = datetime.now().strftime("%d:%m:%y")
fecha

#Se crean los directorios necesarios
#El método "os.makedirs()" se usa para crear un directorio de forma recursiva

try:
    os.makedirs(f'museos/{year}-{month}', exist_ok=True)
    os.makedirs(f'cines/{year}-{month}', exist_ok=True)
    os.makedirs(f'bibliotecas/{year}-{month}', exist_ok=True)
    logging.debug('Los directorios se han creado exitosamente')
except Exception as e:
    logging.exception(f'Ha ocurrido una excepción{e}')

#Descargo los archivos correspondientes a Museos, Cines y Bibliotecas

try:
    if os.path.isfile(f'museos-{day}-{month}-{year}.csv') is True:
        os.remove(f'museos-{day}-{month}-{year}.csv')

    if os.path.isfile(f'cines-{day}-{month}-{year}.csv') is True:
        os.remove(f'cines-{day}-{month}-{year}.csv')

    if os.path.isfile(f'bibliotecas-{day}-{month}-{year}.csv') is True:
        os.remove(f'bibliotecas-{day}-{month}-{year}.csv')
    logging.debug('No se ha encontrado el archivo')
except Exception as e:
    logging.exception(f'Ha ocurrido una excepción {e}')
try:
    r_museos = requests.get(url_museos)

    open(f'museos/{year}-{month}/ museos-{day}-{month}-{year}.csv'
         'wb').write(r_museos.content)

    r_cines = requests.get(url_cines)
    open(f'cines/{year}-{month}/ cines-{day}-{month}-{year}.csv'
         'wb').write(r_cines.content)

    r_biblio = requests.get(url_biblio)
    open(f'bibliotecas/{year}-{month}/ bibliotecas-{day}-{month}-{year}.csv'
         'wb').write(r_biblio.content)
    logging.debug('Se han cargado los archivos correctamente')
except Exception as e:
    logging.exception(f'Ha ocurrido una excepción{e}')

#Se realiza la conexión a la Base de Datos

postgres = {'DB_USER':Config('DB_USER'),
              'BD_PASSWORD':Config('BD_PASSWORD'),
              'BD_HOST':Config('BD_HOST'),
              'DB_PORT ':Config('BD_PORT'),
              'DB_NAME':Config('DB_NAME')
             }
try:
    engine = create_engine('postgresql://postgres:1234@localhost:5432/ChallengeAlkemy')
    logging.debug("La conexión se ha efectuado exitosamente :)")
except IOError:
    logging.exception("No se ha podido realizar la conexión :(")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, blind=engine)
Base = declarative_base()


#PROCESAMIENTOS DE LOS DATOS

#Procesamiento de los datos: Normalización

museos_df = pd.read_csv(r'C:\Users\kgabr\PycharmProjects\Alkemy-Challenge\museos\2022-January\museos-2022-enero.csv', encoding='latin-1', sep='delimiter', header=None, engine='python')
museos = museos_df.drop(columns=['espacio_cultural_id', 'observaciones', 'codigo_indicativo_telefono',
                                 'latitud', 'longitud',
                                 'fuente', 'juridisccion', 'anio_de_creacion', 'descripcion_de_patrimonio',
                                 'anio_de_inauguracion'], axis = '1',  inplace=True, errors='raise')

museos.columns = ['id_provincia', 'cod_localidad', 'provincia', 'localidad', 'nombre',
                  'domicilio', 'codigo postal', 'numero de telefono', 'mail', 'web']

cines_df = pd.read_csv(r'C:\Users\kgabr\PycharmProjects\Alkemy-Challenge\cines\2022-January\ cine-2022-enero.csv', encoding='latin-1', sep='delimiter', header=None, engine='python')

cines = cines_df.drop(columns=['Observaciones', 'Departamento', 'Piso', 'cod_area',
                               'Información adicional', 'Latitud', 'Longitud',
                               'TipoLatitudLongitud', 'Fuente', 'tipo_gestion', 'Pantallas',
                               'Butacas', 'espacio_INCAA', 'año_actualizacion'],  axis = '1',  inplace=True, errors='raise')
cines.columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoría',
                 'provincia', 'localidad', 'nombre', 'domicilio', 'codigo postal',
                 'numero de telefono', 'mail', 'web']

biblio_df = pd.read_csv(r'C:\Users\kgabr\PycharmProjects\Alkemy-Challenge\bibliotecas\2022-January\ biblioteca_popular-2022-enero.csv', encoding='latin-1', sep='delimiter', header=None, engine='python')
bibliotecas = biblio_df.drop(columns=['Observacion', 'Subcategoria', 'Departamento', 'Piso', 'Cod_tel',
                                      'Información adicional', 'Latitud', 'Longitud',
                                      'TipoLatitudLongitud', 'Fuente', 'Tipo_gestion', 'año_inicio',
                                      'Año_actualizacion'],  axis = '1',  inplace=True, errors='raise')

bibliotecas.columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoría',
                       'provincia', 'localidad', 'nombre', 'domicilio', 'codigo postal',
                       'numero de telefono', 'mail', 'web']

#Procesamiento de los datos: creación de una tabla única con los datos de Museos, Cines y Bibliotecas

tabla_unica = pd.concat([bibliotecas, museos, cines], axis=0)
tabla_unica.head()

#Procesamiento de los datos: tabla con cantidad de registros totales por CATEGORÍA

tabla_categorias = tabla_unica.groupby('categoría').size()
tabla_categorias.loc['Fecha de Carga'] = pd.to_datetime(date, format='%d:%m:%y', infer_datetime_format=True)
tabla_categorias

#Procesamiento de los datos: tabla con cantidad de registros totales por FUENTE

tabla_fuentes = pd.concat([biblio_df['Fuente'], cines_df['Fuente'], museos_df['fuente']])
tabla_fuentes.loc['Fecha de Carga'] = pd.to_datetime(date, format='%d:%m:%y', infer_datetime_format=True)
tabla_fuentes

#Procesamiento de los datos: tabla con cantidad de registros totales por PROVINCIA Y CATEGORÍA

tabla_prov_cat = tabla_unica.groupby(['provincia', 'categoría']).size()
tabla_prov_cat.loc['Fecha de Carga'] = pd.to_datetime(date, format='%d:%m:%y', infer_datetime_format=True)
tabla_prov_cat

#Procesamiento de los datos: tabla cde CINES con Provincia, Cant. de pantallas, Cant. de butacas y Cant. de espacios INCAA

tabla_detalle_cines = cines_df.groupby(['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']).size()
tabla_detalle_cines.loc['Fecha de Carga'] = pd.to_datetime(date, format='%d:%m:%y', infer_datetime_format=True)

#Actualización de la base de datos

tabla_categorias.to_sql('tabla_categorias', con=engine, index=False, if_exists="replace")
tabla_fuentes.to_sql('tabla_fuentes', con=engine, index=False, if_exists="replace")
tabla_prov_cat.to_sql('tabla_prov_cat', con=engine, index=False, if_exists="replace")
tabla_detalle_cines.to_sql('tabla_detalle_cines', con=engine, index=False, if_exists="replace")

