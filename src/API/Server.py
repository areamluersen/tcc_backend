from flask import Flask
from flask_restful import Resource, Api, reqparse
from src.API.Municipio import Municipios
import pandas as pd
import ast
app = Flask(__name__)
api = Api(app)



api.add_resource(Municipios, '/municipios')


if __name__ == '__main__':
    app.run()  # run our Flask app