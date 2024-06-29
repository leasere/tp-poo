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
            # print(df_clean.columns)

            return df_clean

        except Exception as ex:
            print(f"Error al limpiar datos: {ex}")
            return None



    @classmethod
    def cargar_datos(cls, df):
    
        try:
            if Obra.select().count() == 0:
                for i, row in df.iterrows():

                    entorno, i = Entorno.get_or_create(nombre=row['entorno'])
                    barrio, i = Barrio.get_or_create(nombre=row['barrio'])
                    comuna, i = Comuna.get_or_create(nombre=row['comuna'])
                    contratacion_tipo, i = Contratacion_tipo.get_or_create(nombre=row['contratacion_tipo'])
                    etapa, i = Etapa.get_or_create(nombre=row['etapa'])
                    licitacion_anio, i = Licitacion_anio.get_or_create(nombre=row['licitacion_anio'])
                    tipo, i = Tipo.get_or_create(nombre=row['tipo'])
                    area_responsable, i = Area_responsable.get_or_create(nombre=row['area_responsable'])

                    Obra.create(
                        entorno = entorno,
                        nombre = row['nombre'],
                        etapa = etapa,
                        tipo = tipo,
                        area_responsable = area_responsable,
                        descripcion = row['descripcion'],
                        monto_contrato = row['monto_contrato'],
                        comuna = comuna,
                        barrio = barrio,
                        direccion = row['direccion'],
                        lat = row['lat'],
                        lng = row['lng'],
                        fecha_inicio = row['fecha_inicio'],
                        fecha_fin_inicial = row['fecha_fin_inicial'],
                        plazo_meses = row['plazo_meses'],
                        porcentaje_avance = row['porcentaje_avance'],
                        licitacion_oferta_empresa = row['licitacion_oferta_empresa'],
                        licitacion_anio = licitacion_anio,
                        contratacion_tipo = contratacion_tipo,
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
        try:
            entorno = input("Ingrese el entorno de la obra: ") 
            if not Obra.select().where(Obra.entorno == entorno).exists():
                print("No es un tipo de contratación válido")
            else:
                entorno, i = Entorno.get_or_create(nombre=entorno)
    
            nombre = input("Ingrese el nombre: ") 
            tipo = input("Ingrese el tipo de obra: ") 

            if not Obra.select().where(Obra.tipo == tipo).exists():
                print("No es un tipo de obra válido")
            else:
                tipo, i = Barrio.get_or_create(nombre=tipo)
    
            area_responsable = input("Ingrese el área responsable: ") 

            if not Obra.select().where(Obra.area_responsable == area_responsable).exists():
                print("No es un tipo de contratación válido")
            else:
                area_responsable, i = Area_responsable.get_or_create(nombre=area_responsable)
    
            descripcion = input("Ingrese la descripción: ") 
            monto_contrato = input("Ingrese el monto del contrato: ") 
    
            comuna = input("Ingrese la comuna: ") 

            if not Obra.select().where(Obra.comuna == comuna).exists():
                print("No es un tipo de contratación válido")
            else:
                comuna, i = Comuna.get_or_create(nombre=comuna)
    
            barrio = input("Ingrese el barrio: ") 

            if not Obra.select().where(Obra.barrio == barrio).exists():
                print("No es un tipo de contratación válido")
            else:
                barrio, i = Barrio.get_or_create(nombre=barrio)
    
            direccion = input("Ingrese la dirección: ") 
            lat = input("Ingrese la latitud: ") 
            lng = input("Ingrese la longitud: ") 
            fecha_inicio = input("Ingrese la fecha de inicio: ") 
            fecha_fin_inicial = input("Ingrese la fecha de fin inicial: ") 
            plazo_meses = input("Ingrese el plazo en meses: ") 
            porcentaje_avance = input("Ingrese el porcentaje de avance: ") 
            licitacion_oferta_empresa = input("Ingrese la licitación/empresa ofertante: ") 
    
            licitacion_anio = input("Ingrese el año de licitación: ") 

            if not Obra.select().where(Obra.licitacion_anio == licitacion_anio).exists():
                print("No es un tipo de contratación válido")
            else:
                licitacion_anio, i = Licitacion_anio.get_or_create(nombre=licitacion_anio)
    
            contratacion_tipo = input("Ingrese el tipo de contratación: ") 
            # contratacion_tipo, i = Contratacion_tipo.get(Contratacion_tipo.tipo == contratacion_tipo_nombre)

            if not Obra.select().where(Obra.contratacion_tipo == contratacion_tipo).exists():
                print("No es un tipo de contratación válido")
            else:
                contratacion_tipo, i = Contratacion_tipo.get_or_create(nombre=contratacion_tipo)
    
            nro_contratacion = input("Ingrese el número de contratación: ") 
            cuit_contratista = input("Ingrese el CUIT del contratista: ") 
            beneficiarios = input("Ingrese los beneficiarios: ") 
            mano_obra = input("Ingrese la mano de obra: ") 
            compromiso = input("Ingrese el compromiso: ") 
            destacada = input("Ingrese si es destacada: ") 
            ba_elige = input("Ingrese si es elegida por BA: ") 
            link_interno = input("Ingrese el link interno: ") 
            pliego_descarga = input("Ingrese el pliego de descarga: ") 
            financiamiento = input("Ingrese el financiamiento: ") 
    
            estado = input("Ingrese el estado de la obra: ") 
    
            nueva_obra = Obra.create( 
                entorno=entorno, 
                nombre=nombre,
                tipo=tipo, 
                area_responsable=area_responsable, 
                descripcion=descripcion, 
                monto_contrato=monto_contrato, 
                comuna=comuna, 
                barrio=barrio, 
                direccion=direccion, 
                lat=lat, 
                lng=lng, 
                fecha_inicio=fecha_inicio, 
                fecha_fin_inicial=fecha_fin_inicial, 
                plazo_meses=plazo_meses, 
                porcentaje_avance=porcentaje_avance, 
                licitacion_oferta_empresa=licitacion_oferta_empresa, 
                licitacion_anio=licitacion_anio, 
                contratacion_tipo=contratacion_tipo,
                nro_contratacion=nro_contratacion, 
                cuit_contratista=cuit_contratista, 
                beneficiarios=beneficiarios, 
                mano_obra=mano_obra, 
                compromiso=compromiso, 
                destacada=destacada, 
                ba_elige=ba_elige, 
                link_interno=link_interno, 
                pliego_descarga=pliego_descarga, 
                financiamiento=financiamiento, 
                estado=estado, 
                etapa="Proyecto" 
            ) 
            
            nueva_obra.save()
            print("Nueva obra creada correctamente")
            if isinstance(nueva_obra, Obra):
                print("El objeto es una instancia de GestionarObraImplementacion")
            else:
                print("El objeto no es una instancia de GestionarObraImplementacion")
            return nueva_obra
            
        except Exception as e:
            print(f"Error al crear la nueva obra: {e}")

    def obtener_indicadores(cls):
        pass


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
    obra1 = GestionarObraImplementacion.nueva_obra()
    # obra2 = GestionarObraImplementacion.nueva_obra()

    if isinstance(obra1, Obra):
        print("El objeto es una instancia de Obra")
    else:
        print("El objeto no es una instancia de Obra")
   
    def pasar_etapas(obra_1):
        obra_1.nuevo_proyecto()
        obra_1.iniciar_contratacion("Convenio", 2000)
        obra_1.adjudicar_obra()
        obra_1.iniciar_obra("SI", "10/10/9663", "10/10/1990", "Préstamo BIRF 0303-AR", 2)
        obra_1.actualizar_porcentaje_avance(50)
        obra_1.finalizar_obra()
        obra_1.rescindir_obra()

        # obra_2.nuevo_proyecto()
        # obra_2.iniciar_contratacion()
        # obra_2.adjudicar_obra()
        # obra_2.iniciar_obra()
        # obra_2.actualizar_porcentaje_avance(25)
        # obra_2.finalizar_obra()

    # obra1.iniciar_contratacion("Convenio", 2000)

    pasar_etapas(obra1)
    # GestionarObraImplementacion.obtener_indicadores()