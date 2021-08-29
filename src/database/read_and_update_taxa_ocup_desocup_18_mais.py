import pandas as pd
from municipios_exec_bd import get_all_municipios, get_connection
import numpy as np
from threading import Thread
from datetime import datetime

def read_excel():
    return pd.read_excel(r"C:\faculdade\tcc\fontes_de_dados\5-taxa_ocupacao_desocupacao_18_ou_mais.xlsx", skiprows=0)  # use r before absolute file path

def update_idhm_per_capita_municipio(ibge_municipio, idhm, column):
    connection = get_connection()
    connection.execute_sql(f"UPDATE public.municipio SET {column}={idhm} WHERE municipio={ibge_municipio};")
    connection.close_connection()


def update_idhm_municipios(municipios, thread_name):
    idhm_file = read_excel()
    for municipio in municipios:
        filter_municipio_string = municipio[1].strip() + f" ({municipio[2]})"
        munFromFile = idhm_file[idhm_file['Territorialidades'] == filter_municipio_string]

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        try:
            tx_ocupacao_18_anos_ou_mais_2010 = munFromFile["Taxa de atividade - 18 anos ou mais de idade 2010"][munFromFile.index[0]]
            tx_desocupacao_18_anos_ou_mais_2010 = munFromFile["Taxa de desocupação - 18 anos ou mais de idade 2010"][munFromFile.index[0]]
            print(current_time, ' - ', municipio[0], '-', municipio[1], ' - In ', thread_name)
            update_idhm_per_capita_municipio(municipio[0], tx_ocupacao_18_anos_ou_mais_2010, "tx_ocupacao_18_anos_ou_mais_2010")
            update_idhm_per_capita_municipio(municipio[0], tx_desocupacao_18_anos_ou_mais_2010, "tx_desocupacao_18_anos_ou_mais_2010")
        except:
            print(current_time, " - ", "Municipio não encontrado: ", municipio[0], '-', municipio[1])



# 1- Ler municipios. OK
# 2- para cada código(nesse caso nome) de município pegar registros do xlsx
# 3- Para o ano 2010 buscar a idhm desejado.
# 5- Criar coluna no banco de dados para salvar os idhms - OK
# 5- Salvar na coluna do banco de cada município o valor da idhm per capita

def split_municipios():
    municipios = get_all_municipios(" m.municipio, m.name, m.uf_code")
    splited = np.array_split(municipios, 10)
    return splited


def count_municipios_threading():
    municipios_splited = split_municipios()
    for i in range(len(municipios_splited)):
        t = Thread(target=update_idhm_municipios, args=(municipios_splited[i], f'Thread {str(i)}'))
        t.start()

count_municipios_threading()