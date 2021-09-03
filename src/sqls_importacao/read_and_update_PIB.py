import pandas as pd
from src.database.municipios_exec_bd import get_all_municipios, get_connection
import numpy as np
from threading import Thread
from datetime import datetime

# print(xls['Nome do Município'])

def read_excel():
    return pd.read_excel(r"C:\faculdade\tcc\fontes_de_dados\1-PIB dos Municípios - base de dados 2010-2018.xls", skiprows=0)  # use r before absolute file path

def update_pib_municipio(ibge_municipio, year, pib):
    connection = get_connection()
    connection.execute_sql(f"UPDATE public.municipio_antropometria_{year}_5_10_anos SET pib_x_1000={pib} WHERE municipio={ibge_municipio};")
    connection.close_connection()

def update_pib_per_capita_municipio(ibge_municipio, year, pib_per_capita):
    connection = get_connection()
    connection.execute_sql(f"UPDATE public.municipio_antropometria_{year}_5_10_anos SET pib_per_capita={pib_per_capita}	WHERE municipio={ibge_municipio};")
    connection.close_connection()



def update_pib_municipios(municipios, thread_name):
    pib_file = read_excel()
    for municipio in municipios:
        mun = pib_file[pib_file['Código do Município'] == municipio[0]]
        ano2013 = mun[mun['Ano'] == 2013]
        pib_2013_correntes_1000 = ano2013["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2013.index[0]]
        pib_per_capita_2013 = ano2013["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2013.index[0]]

        ano2014 = mun[mun['Ano'] == 2014]
        pib_2014_correntes_1000 = ano2014["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2014.index[0]]
        pib_per_capita_2014 = ano2014["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2014.index[0]]

        ano2015 = mun[mun['Ano'] == 2015]
        pib_2015_correntes_1000 = ano2015["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2015.index[0]]
        pib_per_capita_2015 = ano2015["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2015.index[0]]

        ano2016 = mun[mun['Ano'] == 2016]
        pib_2016_correntes_1000 = ano2016["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2016.index[0]]
        pib_per_capita_2016 = ano2016["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2016.index[0]]

        ano2017 = mun[mun['Ano'] == 2017]
        pib_2017_correntes_1000 = ano2017["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2017.index[0]]
        pib_per_capita_2017 = ano2017["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2017.index[0]]

        ano2018 = mun[mun['Ano'] == 2018]
        pib_2018_correntes_1000 = ano2018["Produto Interno Bruto, a preços correntes (R$ 1.000)"][ano2018.index[0]]
        pib_per_capita_2018 = ano2018["Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"][ano2018.index[0]]

        media_incremento_pib = (pib_2018_correntes_1000 - pib_2017_correntes_1000 + pib_2017_correntes_1000 - pib_2016_correntes_1000 + pib_2016_correntes_1000 - pib_2015_correntes_1000
         + pib_2015_correntes_1000 - pib_2014_correntes_1000 + pib_2014_correntes_1000 - pib_2013_correntes_1000) / 5

        media_incremento_pib_per_capita = (pib_per_capita_2018 - pib_per_capita_2017 + pib_per_capita_2017 - pib_per_capita_2016 + pib_per_capita_2016 - pib_per_capita_2015
                                           + pib_per_capita_2015 - pib_per_capita_2014 + pib_per_capita_2014 - pib_per_capita_2013) / 5

        pib_2019_correntes_1000 = pib_2018_correntes_1000 + media_incremento_pib
        pib_2020_correntes_1000 = pib_2019_correntes_1000 + media_incremento_pib
        pib_per_capita_2019 = pib_per_capita_2018 + media_incremento_pib_per_capita
        pib_per_capita_2020 = pib_per_capita_2019 + media_incremento_pib_per_capita

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(current_time, ' - ', municipio[0], '-', ano2013["Nome do Município"][ano2013.index[0]], ' - In ', thread_name)
        update_pib_municipio(municipio[0], 2015, pib_2015_correntes_1000)
        update_pib_municipio(municipio[0], 2016, pib_2016_correntes_1000)
        update_pib_municipio(municipio[0], 2017, pib_2017_correntes_1000)
        update_pib_municipio(municipio[0], 2018, pib_2018_correntes_1000)
        update_pib_municipio(municipio[0], 2019, pib_2019_correntes_1000)
        update_pib_municipio(municipio[0], 2020, pib_2020_correntes_1000)

        update_pib_per_capita_municipio(municipio[0], 2015, pib_per_capita_2015)
        update_pib_per_capita_municipio(municipio[0], 2016, pib_per_capita_2016)
        update_pib_per_capita_municipio(municipio[0], 2017, pib_per_capita_2017)
        update_pib_per_capita_municipio(municipio[0], 2018, pib_per_capita_2018)
        update_pib_per_capita_municipio(municipio[0], 2019, pib_per_capita_2019)
        update_pib_per_capita_municipio(municipio[0], 2020, pib_per_capita_2020)


        # print('Media Incremento PIB: ', media_incremento_pib)
        # print('2013: ', pib_2013_correntes_1000)
        # print('2014: ', pib_2014_correntes_1000)
        # print('2015: ', pib_2015_correntes_1000)
        # print('2016: ', pib_2016_correntes_1000)
        # print('2017: ', pib_2017_correntes_1000)
        # print('2018: ', pib_2018_correntes_1000)
        # print('2019: ', pib_2019_correntes_1000)
        # print('2020: ', pib_2020_correntes_1000)
        #
        # print('Media Incremento PIB per capita: ', media_incremento_pib_per_capita)
        # print('2013: ', pib_per_capita_2013)
        # print('2014: ', pib_per_capita_2014)
        # print('2015: ', pib_per_capita_2015)
        # print('2016: ', pib_per_capita_2016)
        # print('2017: ', pib_per_capita_2017)
        # print('2018: ', pib_per_capita_2018)
        # print('2019: ', pib_per_capita_2019)
        # print('2020: ', pib_per_capita_2020)


# 1- Ler municipios. OK
# 2- para cada código de município pegar registros do xlsx OK
# 3- Para cada ano de interesse buscar o pib desejado. OK
# 4- Calcular o acréscimo anual médio do PIB. Aplicar 2018 + acréscimo em 2019 e 2019 + acréscimo em 2020 OK
# 5- Criar coluna no banco de dados para salvar os PIBs OK
# 5- Salvar na coluna do banco de cada município o valor do PIB OK

def split_municipios():
    municipios = get_all_municipios()
    splited = np.array_split(municipios, 10)
    return splited


def count_municipios_threading():
    municipios_splited = split_municipios()
    for i in range(len(municipios_splited)):
        t = Thread(target=update_pib_municipios, args=(municipios_splited[i], f'Thread {str(i)}'))
        t.start()

count_municipios_threading()