from flask_restful import Resource
from src.database.municipios_exec_bd import get_municipio_identificar


class Municipio(Resource):

    def get(self, ibge):
        print(ibge)
        data = get_municipio_identificar(ibge)
        return data[0][0], 200
