import pandas as pd
import numpy as np
from abc import ABC
from abc import abstractmethod
from peewee import *
from modelo_orm import Obra


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
            df = pd.read_csv(file_path)
            print("\n ### Datos extraídos ###")
            # print(df.head())
            return df
        except pd.errors.ParserError as e:
            print(f"Error al analizar el archivo CSV: {e}")
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
        # Verifica las columnas presentes en el DataFrame antes de manipularlo
        print(df.columns)

        # Convertir tipos de datos y limpiar NaNs como lo hiciste antes
        # df['id'] = pd.to_numeric(df['id'], errors='coerce')
        # Resto de las conversiones y limpiezas...

        return df 

    @classmethod
    def cargar_datos(cls, df):
        pass
        # Obra.create(name= "Nombre de una obra")
        # for _, row in df.iterrows():
        #     Obra.create(nombre=row['nombre'])
        # print("Datos cargados en la base de datos")

    @classmethod
    def nueva_obra(cls):
        nombre = input("Ingrese el nombre de la obra: ")
        nueva_obra = Obra.create(nombre=nombre)
        print("Nueva obra creada:")
        print(nueva_obra)
        return nueva_obra

    @classmethod
    def obtener_indicadores(cls):
        obras = Obra.select()
        print("Indicadores de obras: ", obras)


try:
    sqlite_db.create_tables([Obra])
    print("Tablas creadas correctamente")
except OperationalError as operational_error:
    print("Error al crear las tablas")
    exit()

if __name__ == "__main__":
    GestionarObraImplementacion.conectar_db()
    GestionarObraImplementacion.mapear_orm()
    df = GestionarObraImplementacion.extraer_datos('./observatorio-de-obras-urbanas.csv')
    df_clean = GestionarObraImplementacion.limpiar_datos(df)
    GestionarObraImplementacion.cargar_datos(df_clean)

    # GestionarObraImplementacion.obtener_indicadores()
    #     GestionarObraImplementacion.nueva_obra()