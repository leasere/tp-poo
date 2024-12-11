class Obra():

    def nuevo_proyecto(self):
        self.estado = 'Nuevo Proyecto'
        self.save()

    def iniciar_contratacion(self):
        self.estado = 'En Contratación'
        self.save()

    def adjudicar_obra(self):
        self.estado = 'Obra Adjudicada'
        self.save()

    def iniciar_obra(self):
        self.estado = 'Obra Iniciada'
        self.save()

    def actualizar_porcentaje_avance(self, nuevo_avance):
        if 0 <= nuevo_avance <= 100:
            self.porcentaje_avance = nuevo_avance
            self.save()
        else:
            raise ValueError("El porcentaje de avance debe estar entre 0 y 100")

    def incrementar_plazo(self, dias):
        self.plazo_dias += dias
        self.save()

    def incrementar_mano_obra(self, cantidad):
        self.mano_obra += cantidad
        self.save()

    def finalizar_obra(self):
        self.estado = 'Obra Finalizada'
        self.porcentaje_avance = 100
        self.save()

    def rescindir_obra(self):
        self.estado = 'Obra Rescindida'
        self.save()