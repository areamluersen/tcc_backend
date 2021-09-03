from flask import Flask
from flask_restful import Resource, Api, reqparse
from src.API.Municipios import Municipios
from src.API.Municipio import Municipio
from src.database.municipios_exec_bd import get_municipios_with_data
app = Flask(__name__)
api = Api(app)

api.add_resource(Municipio, '/municipio/<ibge>')
api.add_resource(Municipios, '/municipios')


if __name__ == '__main__':
    app.run()  # run our Flask app