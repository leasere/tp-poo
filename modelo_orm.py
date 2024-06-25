from peewee import *

sqlite_db = SqliteDatabase('./obras_urbanas.db', pragmas={'journal_mode': 'wal'})

try:
    sqlite_db.connect()
    print("Conectado a la base de datos")
except OperationalError as operational_error:
    print("Error al conectar a la base de datos: ", operational_error)
    exit()

class BaseModel(Model):
    class Meta:
        database = sqlite_db

class Obra(BaseModel):
    entorno = CharField()
    nombre = CharField()
    etapa = CharField()
    tipo = CharField()
    area_responsable = CharField()
    descripcion = CharField()
    monto_contrato = CharField()
    comuna = CharField()
    barrio = CharField()
    direccion = CharField()
    lat = CharField()
    lng = CharField()
    fecha_inicio = CharField()
    fecha_fin_inicial = CharField()
    plazo_meses = CharField()
    porcentaje_avance = CharField()
    licitacion_oferta_empresa = CharField()
    licitacion_anio = CharField()
    contratacion_tipo = CharField()
    nro_contratacion= CharField()
    cuit_contratista = CharField()
    beneficiarios = CharField()
    mano_obra = CharField()
    compromiso = CharField()
    destacada = CharField()
    ba_elige = CharField()
    link_interno = CharField()
    pliego_descarga = CharField()
    financiamiento = CharField()
    
