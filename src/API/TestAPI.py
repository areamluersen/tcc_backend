from flask_restful import Resource
from src.database.municipios_exec_bd import get_municipio_identificar


class TestAPI(Resource):

    def get(self, received_code):
        print(received_code)
        return received_code
