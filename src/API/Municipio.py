from flask_restful import Resource
from src.database.municipios_exec_bd import get_municipios_with_data
import json


class Municipios(Resource):
    def get(self):
        data = get_municipios_with_data(2015)
        # municipios = list(data)
        # data = {'municipios': municipios}
        return data[0][0], 200
