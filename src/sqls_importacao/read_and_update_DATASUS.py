import pandas as pd
from src.database.municipios_exec_bd import get_all_municipios, get_connection
import numpy as np
from threading import Thread
from datetime import datetime

# print(xls['Nome do Município'])

def read_excel():
    return pd.read_excel(r"C:\faculdade\tcc\fontes_de_dados\6-DATASUS.xlsx", skiprows=0)  # use r before absolute file path

def update_municipio_datasus(ibge_municipio, year, valor, column):
    connection = get_connection()
    connection.execute_sql(f"UPDATE public.municipio_antropometria_{year}_5_10_anos SET {column}={valor} WHERE municipio={ibge_municipio};")
    connection.close_connection()


def update_municipios_datasus(municipios, thread_name):
    datasus_file = read_excel()
    for municipio in municipios:
        filter_municipio_string = municipio[1].strip() + f" ({municipio[2]})"
        try:
            munFromFile = datasus_file[datasus_file['Territorialidades'] == filter_municipio_string]

            # Taxa bruta de mortalidade
            tx_bruta_mortalidade_2013 = munFromFile["Taxa bruta de mortalidade 2013"][munFromFile.index[0]]
            tx_bruta_mortalidade_2014 = munFromFile["Taxa bruta de mortalidade 2014"][munFromFile.index[0]]
            tx_bruta_mortalidade_2015 = munFromFile["Taxa bruta de mortalidade 2015"][munFromFile.index[0]]
            tx_bruta_mortalidade_2016 = munFromFile["Taxa bruta de mortalidade 2016"][munFromFile.index[0]]
            tx_bruta_mortalidade_2017 = munFromFile["Taxa bruta de mortalidade 2017"][munFromFile.index[0]]

            media_mudancao_mortalidade = (tx_bruta_mortalidade_2017 - tx_bruta_mortalidade_2016 + tx_bruta_mortalidade_2016 - tx_bruta_mortalidade_2015 + tx_bruta_mortalidade_2015 - tx_bruta_mortalidade_2014
             + tx_bruta_mortalidade_2014 - tx_bruta_mortalidade_2013) / 4

            tx_bruta_mortalidade_2018 = round(tx_bruta_mortalidade_2017 + media_mudancao_mortalidade, 2)
            tx_bruta_mortalidade_2019 = round(tx_bruta_mortalidade_2018 + media_mudancao_mortalidade, 2)
            tx_bruta_mortalidade_2020 = round(tx_bruta_mortalidade_2019 + media_mudancao_mortalidade, 2)

            # Taxa de mortalidade infantil
            tx_mortalidade_infantil_2013 = munFromFile["Taxa de mortalidade infantil 2013"][munFromFile.index[0]]
            tx_mortalidade_infantil_2014 = munFromFile["Taxa de mortalidade infantil 2014"][munFromFile.index[0]]
            tx_mortalidade_infantil_2015 = munFromFile["Taxa de mortalidade infantil 2015"][munFromFile.index[0]]
            tx_mortalidade_infantil_2016 = munFromFile["Taxa de mortalidade infantil 2016"][munFromFile.index[0]]
            tx_mortalidade_infantil_2017 = munFromFile["Taxa de mortalidade infantil 2017"][munFromFile.index[0]]

            media_mudancao_mortalidade = (tx_mortalidade_infantil_2017 - tx_mortalidade_infantil_2016 + tx_mortalidade_infantil_2016 - tx_mortalidade_infantil_2015 + tx_mortalidade_infantil_2015 - tx_mortalidade_infantil_2014
                                                     + tx_mortalidade_infantil_2014 - tx_mortalidade_infantil_2013) / 4

            tx_mortalidade_infantil_2018 = round(tx_mortalidade_infantil_2017 + media_mudancao_mortalidade, 2)
            tx_mortalidade_infantil_2019 = round(tx_mortalidade_infantil_2018 + media_mudancao_mortalidade, 2)
            tx_mortalidade_infantil_2020 = round(tx_mortalidade_infantil_2019 + media_mudancao_mortalidade, 2)

            # % de internações por doenças relacionadas ao saneamento ambiental inadequado 2013
            tx_doenca_saneamento_2013 = munFromFile["% de internações por doenças relacionadas ao saneamento ambiental inadequado 2013"][munFromFile.index[0]]
            tx_doenca_saneamento_2014 = munFromFile["% de internações por doenças relacionadas ao saneamento ambiental inadequado 2014"][munFromFile.index[0]]
            tx_doenca_saneamento_2015 = munFromFile["% de internações por doenças relacionadas ao saneamento ambiental inadequado 2015"][munFromFile.index[0]]
            tx_doenca_saneamento_2016 = munFromFile["% de internações por doenças relacionadas ao saneamento ambiental inadequado 2016"][munFromFile.index[0]]
            tx_doenca_saneamento_2017 = munFromFile["% de internações por doenças relacionadas ao saneamento ambiental inadequado 2017"][munFromFile.index[0]]

            media_mudancao_saneamento = (tx_doenca_saneamento_2017 - tx_doenca_saneamento_2016 + tx_doenca_saneamento_2016 - tx_doenca_saneamento_2015 + tx_doenca_saneamento_2015 - tx_doenca_saneamento_2014
                                                     + tx_doenca_saneamento_2014 - tx_doenca_saneamento_2013) / 4

            tx_doenca_saneamento_2018 = round(tx_doenca_saneamento_2017 + media_mudancao_saneamento, 2) if tx_doenca_saneamento_2017 + media_mudancao_saneamento > 0 else 0
            tx_doenca_saneamento_2019 = round(tx_doenca_saneamento_2018 + media_mudancao_saneamento, 2) if tx_doenca_saneamento_2018 + media_mudancao_saneamento > 0 else 0
            tx_doenca_saneamento_2020 = round(tx_doenca_saneamento_2019 + media_mudancao_saneamento, 2) if tx_doenca_saneamento_2019 + media_mudancao_saneamento > 0 else 0

            # % de internações por condições sensíveis à atenção primária 2013
            tx_internacao_atencao_primaria_2013 = munFromFile["% de internações por condições sensíveis à atenção primária 2013"][munFromFile.index[0]]
            tx_internacao_atencao_primaria_2014 = munFromFile["% de internações por condições sensíveis à atenção primária 2014"][munFromFile.index[0]]
            tx_internacao_atencao_primaria_2015 = munFromFile["% de internações por condições sensíveis à atenção primária 2015"][munFromFile.index[0]]
            tx_internacao_atencao_primaria_2016 = munFromFile["% de internações por condições sensíveis à atenção primária 2016"][munFromFile.index[0]]
            tx_internacao_atencao_primaria_2017 = munFromFile["% de internações por condições sensíveis à atenção primária 2017"][munFromFile.index[0]]

            media_mudanca_atencao_primaria = (tx_internacao_atencao_primaria_2017 - tx_internacao_atencao_primaria_2016 + tx_internacao_atencao_primaria_2016 - tx_internacao_atencao_primaria_2015 + tx_internacao_atencao_primaria_2015 - tx_internacao_atencao_primaria_2014
                                                     + tx_internacao_atencao_primaria_2014 - tx_internacao_atencao_primaria_2013) / 4

            tx_internacao_atencao_primaria_2018 = round(tx_internacao_atencao_primaria_2017 + media_mudanca_atencao_primaria, 2) if tx_internacao_atencao_primaria_2017 + media_mudanca_atencao_primaria > 0 else 0
            tx_internacao_atencao_primaria_2019 = round(tx_internacao_atencao_primaria_2018 + media_mudanca_atencao_primaria, 2) if tx_internacao_atencao_primaria_2018 + media_mudanca_atencao_primaria > 0 else 0
            tx_internacao_atencao_primaria_2020 = round(tx_internacao_atencao_primaria_2019 + media_mudanca_atencao_primaria, 2) if tx_internacao_atencao_primaria_2019 + media_mudanca_atencao_primaria > 0 else 0


            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            print(current_time, ' - ', municipio[0], '-', municipio[1], ' - In ', thread_name)
            #update tx_bruta_mortalidade
            update_municipio_datasus(municipio[0], 2015, tx_bruta_mortalidade_2015, "tx_bruta_mortalidade")
            update_municipio_datasus(municipio[0], 2016, tx_bruta_mortalidade_2016, "tx_bruta_mortalidade")
            update_municipio_datasus(municipio[0], 2017, tx_bruta_mortalidade_2017, "tx_bruta_mortalidade")
            update_municipio_datasus(municipio[0], 2018, tx_bruta_mortalidade_2018, "tx_bruta_mortalidade")
            update_municipio_datasus(municipio[0], 2019, tx_bruta_mortalidade_2019, "tx_bruta_mortalidade")
            update_municipio_datasus(municipio[0], 2020, tx_bruta_mortalidade_2020, "tx_bruta_mortalidade")

            # update tx_bruta_mortalidade_infantil
            update_municipio_datasus(municipio[0], 2015, tx_mortalidade_infantil_2015, "tx_bruta_mortalidade_infantil")
            update_municipio_datasus(municipio[0], 2016, tx_mortalidade_infantil_2016, "tx_bruta_mortalidade_infantil")
            update_municipio_datasus(municipio[0], 2017, tx_mortalidade_infantil_2017, "tx_bruta_mortalidade_infantil")
            update_municipio_datasus(municipio[0], 2018, tx_mortalidade_infantil_2018, "tx_bruta_mortalidade_infantil")
            update_municipio_datasus(municipio[0], 2019, tx_mortalidade_infantil_2019, "tx_bruta_mortalidade_infantil")
            update_municipio_datasus(municipio[0], 2020, tx_mortalidade_infantil_2020, "tx_bruta_mortalidade_infantil")

            # update tx_internacao_doencas_saneam_amb_inadequado
            update_municipio_datasus(municipio[0], 2015, tx_doenca_saneamento_2015, "tx_internacao_doencas_saneam_amb_inadequado")
            update_municipio_datasus(municipio[0], 2016, tx_doenca_saneamento_2016, "tx_internacao_doencas_saneam_amb_inadequado")
            update_municipio_datasus(municipio[0], 2017, tx_doenca_saneamento_2017, "tx_internacao_doencas_saneam_amb_inadequado")
            update_municipio_datasus(municipio[0], 2018, tx_doenca_saneamento_2018, "tx_internacao_doencas_saneam_amb_inadequado")
            update_municipio_datasus(municipio[0], 2019, tx_doenca_saneamento_2019, "tx_internacao_doencas_saneam_amb_inadequado")
            update_municipio_datasus(municipio[0], 2020, tx_doenca_saneamento_2020, "tx_internacao_doencas_saneam_amb_inadequado")

            # update tx_internacao_por_cond_sensiveis_atencao_primaria
            update_municipio_datasus(municipio[0], 2015, tx_internacao_atencao_primaria_2015, "tx_internacao_por_cond_sensiveis_atencao_primaria")
            update_municipio_datasus(municipio[0], 2016, tx_internacao_atencao_primaria_2016, "tx_internacao_por_cond_sensiveis_atencao_primaria")
            update_municipio_datasus(municipio[0], 2017, tx_internacao_atencao_primaria_2017, "tx_internacao_por_cond_sensiveis_atencao_primaria")
            update_municipio_datasus(municipio[0], 2018, tx_internacao_atencao_primaria_2018, "tx_internacao_por_cond_sensiveis_atencao_primaria")
            update_municipio_datasus(municipio[0], 2019, tx_internacao_atencao_primaria_2019, "tx_internacao_por_cond_sensiveis_atencao_primaria")
            update_municipio_datasus(municipio[0], 2020, tx_internacao_atencao_primaria_2020, "tx_internacao_por_cond_sensiveis_atencao_primaria")

        except:
            print(current_time, " - ", "Municipio não encontrado: ", municipio[0], '-', municipio[1])



# 1- Ler municipios. OK
# 2- para cada código de município pegar registros do xlsx OK
# 3- Para cada ano de interesse buscar o pib desejado. OK
# 4- Calcular o acréscimo anual médio do PIB. Aplicar 2018 + acréscimo em 2019 e 2019 + acréscimo em 2020 OK
# 5- Criar coluna no banco de dados para salvar os PIBs OK
# 5- Salvar na coluna do banco de cada município o valor do PIB OK

def split_municipios():
    municipios = get_all_municipios(" m.municipio, m.name, m.uf_code")
    splited = np.array_split(municipios, 10)
    return splited


def count_municipios_threading():
    municipios_splited = split_municipios()
    for i in range(len(municipios_splited)):
        t = Thread(target=update_municipios_datasus, args=(municipios_splited[i], f'Thread {str(i)}'))
        t.start()

count_municipios_threading()