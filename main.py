import os
from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from src.API.Municipios import Municipios
from src.API.Municipio import Municipio
from src.API.TestAPI import TestAPI
from src.database.municipios_exec_bd import get_municipios_with_data
app = Flask(__name__)
api = Api(app)
CORS(app)

api.add_resource(Municipio, '/municipio/<ibge>')
api.add_resource(Municipios, '/municipios/<year>')
api.add_resource(TestAPI, '/test/<received_code>')


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", 5000)))