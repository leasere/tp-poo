import pandas as pd
from pandas.errors import ParserError
import numpy as np
import csv
from abc import ABC
from abc import abstractmethod
from peewee import *
from modelo_orm import Obra, Entorno, Etapa, Tipo, Area_responsable, Comuna, Barrio, Licitacion_anio, Contratacion_tipo


sqlite_db = SqliteDatabase('obras_urbanas.db', pragmas={'journal_mode': 'wal'})

class GestionarObra(ABC):

    @classmethod
    @abstractmethod
    def extraer_datos(cls, file_path):
        pass

    @classmethod
    @abstractmethod
    def conectar_db(cls):
        pass

    @classmethod
    @abstractmethod
    def mapear_orm(cls):
        pass

    @classmethod
    @abstractmethod
    def limpiar_datos(cls, df):
        pass

    @classmethod
    @abstractmethod
    def cargar_datos(cls, df):
        pass

    @classmethod
    @abstractmethod
    def nueva_obra(cls):
        pass

    @classmethod
    @abstractmethod
    def obtener_indicadores(cls):
        pass

class GestionarObraImplementacion(GestionarObra):

    @classmethod
    def extraer_datos(cls, file_path):
        try:
            df = pd.read_csv(file_path, sep=';', quotechar='"')
            return df

        except ParserError as e:
            print(f"Error al analizar el archivo CSV: {e}")
            return None
        except ValueError as ve:
            print(f"Error en tipos de datos en el archivo CSV: {ve}")
            return None
        except Exception as ex:
            print(f"Error inesperado al leer el archivo CSV: {ex}")
            return None

    @classmethod
    def conectar_db(cls):
        sqlite_db.connect()
        print("Conectado a la base de datos")

    @classmethod
    def mapear_orm(cls):
        sqlite_db.create_tables([Obra])
        print("Tablas creadas en la base de datos")

    @classmethod
    def limpiar_datos(cls, df):

        try:
            columnas_a_eliminar = [
                ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,',
                'Unnamed: 37', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40',
                'Unnamed: 41', 'Unnamed: 42', 'Unnamed: 43', 'Unnamed: 44',
                'Unnamed: 45', 'Unnamed: 46', 'Unnamed: 47', 'Unnamed: 48',
                'Unnamed: 49', 'Unnamed: 50', 'Unnamed: 51', 'Unnamed: 52',
                'Unnamed: 53', 'Unnamed: 54', 'Unnamed: 55', 'Unnamed: 56',
                'Unnamed: 57', 'Unnamed: 58', 'Unnamed: 59', 'Unnamed: 60',
                'Unnamed: 61', 'Unnamed: 62', 'Unnamed: 63', 'Unnamed: 64',
                'Unnamed: 65', 'Unnamed: 66', 'Unnamed: 67', 'Unnamed: 68',
                'Unnamed: 69', 'Unnamed: 70', 'Unnamed: 71', 'Unnamed: 72',
                'Unnamed: 73', 'Unnamed: 74', 'Unnamed: 75', 'Unnamed: 76',
                'Unnamed: 77', 'Unnamed: 78', 'Unnamed: 79', 'Unnamed: 80',
                'Unnamed: 81', 'Unnamed: 82'
            ]

            df_clean = df.drop(columns=columnas_a_eliminar)
            print(df_clean.columns)

            return df_clean

        except Exception as ex:
            print(f"Error al limpiar datos: {ex}")
            return None



    @classmethod
    def cargar_datos(cls, df):
    
        try:
            for index, row in df.iterrows():

                entorno, index = Entorno.get_or_create(nombre=row['entorno'])

                Obra.create(
                    entorno = entorno,
                    nombre = row['nombre'],
                    etapa = row['etapa'],
                    tipo = row['tipo'],
                    area_responsable = row['area_responsable'],
                    descripcion = row['descripcion'],
                    monto_contrato = row['monto_contrato'],
                    comuna = row['comuna'],
                    barrio = row['barrio'],
                    direccion = row['direccion'],
                    lat = row['lat'],
                    lng = row['lng'],
                    fecha_inicio = row['fecha_inicio'],
                    fecha_fin_inicial = row['fecha_fin_inicial'],
                    plazo_meses = row['plazo_meses'],
                    porcentaje_avance = row['porcentaje_avance'],
                    licitacion_oferta_empresa = row['licitacion_oferta_empresa'],
                    licitacion_anio = row['licitacion_anio'],
                    contratacion_tipo = row['contratacion_tipo'],
                    nro_contratacion = row['nro_contratacion'],
                    cuit_contratista = row['cuit_contratista'],
                    beneficiarios = row['beneficiarios'],
                    mano_obra = row['mano_obra'],
                    compromiso = row['compromiso'],
                    destacada = row['destacada'],
                    ba_elige = row['ba_elige'],
                    link_interno = row['link_interno'],
                    pliego_descarga = row['pliego_descarga'],
                    financiamiento = row['financiamiento'],
                )
            
            print("Datos cargados correctamente en la base de datos.")
        
        except Exception as ex:
            print(f"Error al cargar datos en la base de datos: {ex}")

    @classmethod
    def nueva_obra(cls):
        nombre = input("Ingrese el nombre de la obra: ")
        # nueva_obra = Obra.create(
        #     entorno = row['entorno'],
        #     nombre = row['nombre'],
        #     etapa = row['etapa'],
        #     tipo = row['tipo'],
        #     area_responsable = row['area_responsable'],
        #     descripcion = row['descripcion'],
        #     monto_contrato = row['monto_contrato'],
        #     comuna = row['comuna'],
        #     barrio = row['barrio'],
        #     direccion = row['direccion'],
        #     lat = row['lat'],
        #     lng = row['lng'],
        #     fecha_inicio = row['fecha_inicio'],
        #     fecha_fin_inicial = row['fecha_fin_inicial'],
        #     plazo_meses = row['plazo_meses'],
        #     porcentaje_avance = row['porcentaje_avance'],
        #     licitacion_oferta_empresa = row['licitacion_oferta_empresa'],
        #     licitacion_anio = row['licitacion_anio'],
        #     contratacion_tipo = row['contratacion_tipo'],
        #     nro_contratacion = row['nro_contratacion'],
        #     cuit_contratista = row['cuit_contratista'],
        #     beneficiarios = row['beneficiarios'],
        #     mano_obra = row['mano_obra'],
        #     compromiso = row['compromiso'],
        #     destacada = row['destacada'],
        #     ba_elige = row['ba_elige'],
        #     link_interno = row['link_interno'],
        #     pliego_descarga = row['pliego_descarga'],
        #     financiamiento = row['financiamiento'],
        # )
        # print("Nueva obra creada:")
        # print(nueva_obra)
        # return nueva_obra

    @classmethod
    def obtener_indicadores(cls):
        obras = Obra.select()
        print("Indicadores de obras: ", obras)


try:
    sqlite_db.create_tables([Obra, Entorno, Etapa, Tipo, Area_responsable, Comuna, Barrio, Licitacion_anio, Contratacion_tipo])
    print("Tablas creadas correctamente")
except OperationalError as operational_error:
    print("Error al crear las tablas")
    exit()

if __name__ == "__main__":
    GestionarObraImplementacion.conectar_db()
    GestionarObraImplementacion.mapear_orm()
    path = './observatorio-de-obras-urbanas.csv'
    df = GestionarObraImplementacion.extraer_datos(path)
    df_clean = GestionarObraImplementacion.limpiar_datos(df)
    GestionarObraImplementacion.cargar_datos(df_clean)
    # GestionarObraImplementacion.obtener_indicadores()
    # GestionarObraImplementacion.nueva_obra()