from flask_restful import Resource
from src.database.municipios_exec_bd import get_municipios_with_data_forGeoJSON


class Municipios(Resource):
    def get(self, year=2015):
        data = get_municipios_with_data_forGeoJSON(year)
        # municipios = list(data)
        # data = {'municipios': municipios}
        return data[0][0], 200