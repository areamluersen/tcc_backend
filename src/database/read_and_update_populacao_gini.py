import pandas as pd
from municipios_exec_bd import get_all_municipios, get_connection
import numpy as np
from threading import Thread
from datetime import datetime

def read_excel():
    return pd.read_excel(r"C:\faculdade\tcc\fontes_de_dados\9-populacao_e_gini.xlsx", skiprows=0)  # use r before absolute file path

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
            indice_gini_2010 = munFromFile["Índice de Gini 2010"][munFromFile.index[0]]
            populacao_2010 = munFromFile["População total 2010"][munFromFile.index[0]]
            populacao_10a14_anos_2010 = munFromFile["População de 10  a 14 anos de idade 2010"][munFromFile.index[0]]
            populacao_5a9_anos_m_2010 = munFromFile["População masculina de 5 a 9 anos de idade 2010"][munFromFile.index[0]]
            populacao_5a9_anos_f_2010 = munFromFile["População feminina de 5 a 9 anos de idade 2010"][munFromFile.index[0]]
            print(current_time, ' - ', municipio[0], '-', municipio[1], ' - In ', thread_name)
            update_idhm_per_capita_municipio(municipio[0], indice_gini_2010, "indice_gini_2010")
            update_idhm_per_capita_municipio(municipio[0], populacao_2010, "populacao_2010")
            update_idhm_per_capita_municipio(municipio[0], populacao_10a14_anos_2010, "populacao_10a14_anos_2010")
            update_idhm_per_capita_municipio(municipio[0], populacao_5a9_anos_m_2010, "populacao_5a9_anos_m_2010")
            update_idhm_per_capita_municipio(municipio[0], populacao_5a9_anos_f_2010, "populacao_5a9_anos_f_2010")
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