from peewee import *

sqlite_db = SqliteDatabase('./obras_urbanas.db', pragmas={'journal_mode': 'wal'})

try:
    sqlite_db.connect()
except OperationalError as operational_error:
    print("Error al conectar a la base de datos: ", operational_error)
    exit()

class BaseModel(Model):
    class Meta:
        database = sqlite_db

class Obra(BaseModel):
    name = CharField(max_length=80, unique=True)
    type = CharField(max_length=50)
    description = TextField()