import pandas as pd
from src.database.municipios_exec_bd import get_all_municipios, get_connection
import numpy as np
from threading import Thread
from datetime import datetime

def read_excel():
    return pd.read_excel(r"C:\faculdade\tcc\fontes_de_dados\7-renda_per_capita_censo.xlsx", skiprows=0)  # use r before absolute file path

def update_renda_per_capita_municipio(ibge_municipio, renda_per_capita):
    connection = get_connection()
    connection.execute_sql(f"UPDATE public.municipio SET renda_per_capita_2010_censo={renda_per_capita} WHERE municipio={ibge_municipio};")
    connection.close_connection()


def update_renda_municipios(municipios, thread_name):
    renda_file = read_excel()
    for municipio in municipios:
        filter_municipio_string = municipio[1].strip() + f" ({municipio[2]})"
        munFromFile = renda_file[renda_file['Territorialidades'] == filter_municipio_string]

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        try:
            renda_ano2010 = munFromFile["Renda per capita 2010"][munFromFile.index[0]]
            print(current_time, ' - ', municipio[0], '-', municipio[1], ' - In ', thread_name)
            update_renda_per_capita_municipio(municipio[0], renda_ano2010)
        except:
            print(current_time, " - ", "Municipio não encontrado: ", municipio[0], '-', municipio[1])



# 1- Ler municipios. OK
# 2- para cada código(nesse caso nome) de município pegar registros do xlsx
# 3- Para o ano 2010 buscar a renda desejado.
# 5- Criar coluna no banco de dados para salvar os rendas - OK
# 5- Salvar na coluna do banco de cada município o valor da renda per capita

def split_municipios():
    municipios = get_all_municipios(" m.municipio, m.name, m.uf_code")
    splited = np.array_split(municipios, 10)
    return splited


def count_municipios_threading():
    municipios_splited = split_municipios()
    for i in range(len(municipios_splited)):
        t = Thread(target=update_renda_municipios, args=(municipios_splited[i], f'Thread {str(i)}'))
        t.start()

count_municipios_threading()