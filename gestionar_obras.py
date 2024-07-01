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

            entorno = input("Ingrese el entorno de la obra existente: ")
            entorno = Entorno.select().where(Entorno.nombre == entorno).exists()
            while not entorno:
                entorno = input("No es un entorno válido intente nuevamente: ")
                entorno = Entorno.select().where(Entorno.nombre == entorno).exists()
            entorno, _ = Entorno.get_or_create(nombre=entorno)
            print("¡Ingresado correctamente!")
    
            nombre = input("Ingrese el nombre: ") 

            tipo = input("Ingrese el tipo de obra existente: ")
            tipo = Tipo.select().where(Tipo.nombre == tipo).exists()
            while not tipo:
                tipo = input("No es un tipo válido intente nuevamente: ")
                tipo = Tipo.select().where(Tipo.nombre == tipo).exists()
            tipo, _ = Tipo.get_or_create(nombre=tipo)
            print("¡Ingresado correctamente!")

            area_responsable = input("Ingrese el área responsable existente: ")
            area_responsable = Area_responsable.select().where(Area_responsable.nombre == area_responsable).exists()
            while not area_responsable:
                area_responsable = input("No es un area responsable válido intente nuevamente: ")
                area_responsable = Area_responsable.select().where(Area_responsable.nombre == area_responsable).exists()
            area_responsable, _ = Area_responsable.get_or_create(nombre=area_responsable)
            print("¡Ingresado correctamente!")
    
            descripcion = input("Ingrese la descripción : ") 
            monto_contrato = input("Ingrese el monto del contrato: ")

            comuna = input("Ingrese la comuna existente: ")
            comuna = Comuna.select().where(Comuna.nombre == comuna).exists()
            while not comuna:
                comuna = input("No es un comuna válido intente nuevamente: ")
                comuna = Comuna.select().where(Comuna.nombre == comuna).exists()
            comuna, _ = Comuna.get_or_create(nombre=comuna)
            print("¡Ingresado correctamente!")
    
            barrio = input("Ingrese el barrio existente: ")
            barrio = Barrio.select().where(Barrio.nombre == barrio).exists()
            while not barrio:
                barrio = input("No es un barrio válido intente nuevamente: ")
                barrio = Barrio.select().where(Barrio.nombre == barrio).exists()
            barrio, _ = Barrio.get_or_create(nombre=barrio)
            print("¡Ingresado correctamente!")
    
            direccion = input("Ingrese la dirección: ") 
            lat = input("Ingrese la latitud: ") 
            lng = input("Ingrese la longitud: ") 
            fecha_inicio = input("Ingrese la fecha de inicio: ") 
            fecha_fin_inicial = input("Ingrese la fecha de fin inicial: ") 
            porcentaje_avance = input("Ingrese el porcentaje de avance: ") 
            licitacion_oferta_empresa = input("Ingrese la licitación/empresa ofertante: ") 
    
            licitacion_anio = input("Ingrese el año de licitación existente: ") 
            licitacion_anio = Licitacion_anio.select().where(Licitacion_anio.nombre == licitacion_anio).exists()
            while not licitacion_anio:
                licitacion_anio = input("No es un año de licitación válido intente nuevamente: ")
                licitacion_anio = Licitacion_anio.select().where(Licitacion_anio.nombre == licitacion_anio).exists()
            licitacion_anio, _ = Licitacion_anio.get_or_create(nombre=licitacion_anio)
            print("¡Ingresado correctamente!")
    
            contratacion_tipo = input("Ingrese el tipo de contratación existente: ")
            contratacion_tipo = Contratacion_tipo.select().where(Contratacion_tipo.nombre == contratacion_tipo).exists()
            while not contratacion_tipo:
                contratacion_tipo = input("No es un tipo de contratación válido intente nuevamente: ")
                contratacion_tipo = Contratacion_tipo.select().where(Contratacion_tipo.nombre == contratacion_tipo).exists()
            contratacion_tipo, _ = Contratacion_tipo.get_or_create(nombre=contratacion_tipo)
            print("¡Ingresado correctamente!")
    
            nro_contratacion = input("Ingrese el número de contratación: ") 
            cuit_contratista = input("Ingrese el CUIT del contratista: ") 
            beneficiarios = input("Ingrese los beneficiarios: ") 
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
                plazo_meses=0,
                porcentaje_avance=porcentaje_avance, 
                licitacion_oferta_empresa=licitacion_oferta_empresa, 
                licitacion_anio=licitacion_anio, 
                contratacion_tipo=contratacion_tipo,
                nro_contratacion=nro_contratacion, 
                cuit_contratista=cuit_contratista, 
                beneficiarios=beneficiarios, 
                mano_obra=0, 
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
    obra2 = GestionarObraImplementacion.nueva_obra()

    def pasar_etapas(obra_1, obra_2):
        obra_1.nuevo_proyecto()
        obra_1.iniciar_contratacion("Convenio", 2000)
        obra_1.adjudicar_obra()
        obra_1.iniciar_obra("SI", "8/02/2026", "10/10/2080", "Préstamo BIRF 0303-AR", 2)
        obra_1.actualizar_porcentaje_avance(99)
        obra_1.incrementar_plazo(555)
        obra_1.incrementar_mano_obra(555)
        obra_1.finalizar_obra()
        obra_1.rescindir_obra()

        obra_2.nuevo_proyecto()
        obra_2.iniciar_contratacion("Convenio", 2000)
        obra_2.adjudicar_obra()
        obra_2.iniciar_obra("SI", "09/01/2025", "10/10/2150", "Préstamo BIRF 999-AR", 90)
        obra_2.actualizar_porcentaje_avance(99)
        obra_2.incrementar_plazo(555)
        obra_2.incrementar_mano_obra(555)
        obra_2.finalizar_obra()
        obra_2.rescindir_obra()


    pasar_etapas(obra1, obra2)
    # GestionarObraImplementacion.obtener_indicadores()

class GestionarObra:
    @classmethod
    def obtener_indicadores(cls):
        
        # a. Listado de todas las áreas responsables
        areas_responsables = cls.obtener_areas_responsables()

        # b. Listado de todos los tipos de obra
        tipos_de_obra = cls.obtener_tipos_de_obra()

        # c. Cantidad de obras que se encuentran en cada etapa
        cantidad_por_etapa = cls.cantidad_obras_por_etapa()

        # d. Cantidad de obras y monto total de inversión por tipo de obra
        cantidad_inversion_por_tipo = cls.cantidad_inversion_por_tipo()

        # e. Listado de todos los barrios pertenecientes a las comunas 1, 2 y 3
        barrios_comunas_1_2_3 = cls.obtener_barrios_comunas_1_2_3()

        # f. Cantidad de obras finalizadas y monto total de inversión en la comuna 1
        obras_finalizadas_comuna_1 = cls.obras_finalizadas_en_comuna(1)
        cantidad_finalizadas_comuna_1 = len(obras_finalizadas_comuna_1)
        inversion_total_comuna_1 = sum(obra.inversion for obra in obras_finalizadas_comuna_1)

        # g. Cantidad de obras finalizadas en un plazo menor o igual a 24 meses
        obras_finalizadas_24_meses = cls.obras_finalizadas_en_plazo(24)
        cantidad_finalizadas_24_meses = len(obras_finalizadas_24_meses)

        # h. Porcentaje total de obras finalizadas
        total_obras = cls.total_obras()
        obras_finalizadas = cls.obras_finalizadas()
        porcentaje_obras_finalizadas = (obras_finalizadas / total_obras) * 100 if total_obras > 0 else 0

        # i. Cantidad total de mano de obra empleada
        mano_de_obra_total = cls.mano_de_obra_total()

        # j. Monto total de inversión
        inversion_total = cls.inversion_total()

        # Mostrar resultados por consola
        print("a. Listado de todas las áreas responsables:")
        for area in areas_responsables:
            print(area)

        print("\nb. Listado de todos los tipos de obra:")
        for tipo in tipos_de_obra:
            print(tipo)

        print("\nc. Cantidad de obras que se encuentran en cada etapa:")
        for etapa, cantidad in cantidad_por_etapa.items():
            print(f"{etapa}: {cantidad}")

        print("\nd. Cantidad de obras y monto total de inversión por tipo de obra:")
        for tipo, info in cantidad_inversion_por_tipo.items():
            cantidad = info['cantidad']
            inversion = info['inversion']
            print(f"{tipo}: {cantidad} obras, Inversión total: {inversion}")

        print("\ne. Listado de todos los barrios pertenecientes a las comunas 1, 2 y 3:")
        for barrio in barrios_comunas_1_2_3:
            print(barrio)

        print("\nf. Cantidad de obras finalizadas y monto total de inversión en la comuna 1:")
        print(f"Obras finalizadas: {cantidad_finalizadas_comuna_1}, Inversión total: {inversion_total_comuna_1}")

        print("\ng. Cantidad de obras finalizadas en un plazo menor o igual a 24 meses:")
        print(f"Obras finalizadas en <= 24 meses: {cantidad_finalizadas_24_meses}")

        print("\nh. Porcentaje total de obras finalizadas:")
        print(f"{porcentaje_obras_finalizadas}%")

        print("\ni. Cantidad total de mano de obra empleada:")
        print(mano_de_obra_total)

        print("\nj. Monto total de inversión:")
        print(inversion_total)

GestionarObra.obtener_indicadores()
