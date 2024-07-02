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


class Entorno(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()
   
class Etapa(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Licitacion_anio(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Contratacion_tipo(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Tipo(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Area_responsable(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Comuna(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Barrio(BaseModel):
    id = IntegerField(primary_key=True)
    nombre = TextField()

class Obra(BaseModel):
    id = IntegerField(primary_key=True)

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
    plazo_meses = IntegerField(null=True)
    porcentaje_avance = CharField()
    licitacion_oferta_empresa = CharField()
    licitacion_anio = CharField()
    contratacion_tipo = CharField()
    nro_contratacion= CharField()
    cuit_contratista = CharField()
    beneficiarios = CharField()
    mano_obra = IntegerField(null=True)
    compromiso = CharField()
    destacada = CharField()
    ba_elige = CharField()
    link_interno = CharField()
    pliego_descarga = CharField()
    financiamiento = CharField()

    entorno = ForeignKeyField(Entorno, backref='obras')
    area_responsable = ForeignKeyField(Area_responsable, backref='obras')
    barrio = ForeignKeyField(Barrio, backref='obras')
    comuna = ForeignKeyField(Comuna, backref='obras')
    contratacion_tipo = ForeignKeyField(Contratacion_tipo, backref='obras')
    etapa = ForeignKeyField(Etapa, backref='obras')
    licitacion_anio = ForeignKeyField(Licitacion_anio, backref='obras')
    tipo = ForeignKeyField(Tipo, backref='obras')

    def nuevo_proyecto(self):
        proyecto, created = Etapa.get_or_create(nombre="Proyecto")
        self.etapa = proyecto
        self.save()

    def iniciar_contratacion(self, tipo_contratacion, nro_contratacion):
        tipo_contratacion_obj, created = Contratacion_tipo.get_or_create(nombre=tipo_contratacion)
        self.contratacion_tipo = tipo_contratacion_obj
        self.nro_contratacion = nro_contratacion
        self.save()

    def adjudicar_obra(self):
        adjudicacion, created = Etapa.get_or_create(nombre="Adjudicación")
        self.etapa = adjudicacion
        self.save()

    def iniciar_obra(self, destacada, fecha_inicio, fecha_final, financiamiento_1, mano_de_obra):
        try:
            inicio_etapa, created = Etapa.get_or_create(nombre="Inicio de obra")

            self.destacada = destacada
            self.fecha_inicio = fecha_inicio
            self.fecha_fin_inicial = fecha_final
            self.financiamiento = financiamiento_1
            self.mano_obra = mano_de_obra
            self.etapa = inicio_etapa
            self.save()
            print(f"Obra '{self.nombre}' iniciada correctamente en la etapa '{self.etapa.nombre}'")
        except Exception as ex:
            print(f"Error al iniciar la obra: {ex}")

    def actualizar_porcentaje_avance(self, porcentaje):
        self.porcentaje_avance = porcentaje
        self.save()

    def incrementar_plazo(self, meses):
        self.plazo_meses += meses
        self.save()

    def incrementar_mano_obra(self, cantidad):
        self.mano_obra += cantidad
        self.save()

    def finalizar_obra(self):
        finalizacion, created = Etapa.get_or_create(nombre="Finalización")
        self.etapa = finalizacion
        self.porcentaje_avance = 100
        self.save()

    def rescindir_obra(self):
        rescindir, created = Etapa.get_or_create(nombre="Rescindida")
        self.etapa = rescindir
        self.save()
