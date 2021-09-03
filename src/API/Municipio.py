from flask_restful import Resource
from src.database.municipios_exec_bd import get_municipio_with_data


class Municipio(Resource):

    def get(self, ibge):
        print(ibge)
        data = get_municipio_with_data(2015, ibge)
        return data[0][0], 200
