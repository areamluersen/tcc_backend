from src.database.Connection import Connection
import numpy as np
from threading import Thread
from datetime import datetime


def get_connection():
    return Connection("localhost", 5432, "postgres", "postgres", "sldak47")


def get_all_municipios_by_year_count_is_zero(year="2015"):
    connection = get_connection()
    municipios = connection.execute_get_sql(
        f"select m.municipio from public.municipio_antropometria_{year}_5_10_anos m where m.total_registros = 0")
    connection.close_connection()
    return municipios


def get_all_municipios(fields=" m.municipio "):
    connection = get_connection()
    municipios = connection.execute_get_sql(f"select {fields} from public.municipio m order by m.municipio ")
    connection.close_connection()
    return municipios


def get_municipios_with_data(year=2015):
    connection = get_connection()
    municipios = connection.execute_get_sql(f"""
    select jsonb_agg(t) from (
    select 
json_build_object (
	'name', m."name",
	'tx_registros_m', case when ma.total_registros_m > 0 then round(cast((ma.total_registros_m::float / ma.total_registros::float) * 100 as numeric), 2) else 0 end,
	'tx_registros_f', case when ma.total_registros_f > 0 then round(cast((ma.total_registros_f::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_magreza', case when ma.magreza_m > 0 then round(cast((ma.magreza_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_eutrofia', case when ma.eutrofia_m > 0 then round(cast((ma.eutrofia_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_risco_sobrepeso', case when ma.risco_sobrepeso_m > 0 then round(cast((ma.risco_sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_sobrepeso', case when ma.sobrepeso_m > 0 then round(cast((ma.sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_obesidade', case when ma.obesidade_obesidade_grave_m > 0 then round(cast((ma.obesidade_obesidade_grave_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_magreza', case when ma.magreza_f > 0 then round(cast((ma.magreza_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_eutrofia', case when ma.eutrofia_f > 0 then round(cast((ma.eutrofia_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_risco_sobrepeso', case when ma.risco_sobrepeso_f > 0 then round(cast((ma.risco_sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_sobrepeso', case when ma.sobrepeso_f > 0 then round(cast((ma.sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_obesidade', case when ma.obesidade_obesidade_grave_f > 0 then round(cast((ma.obesidade_obesidade_grave_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'uf_code', m.uf_code,
	'renda_per_capita_2010_censo', m.renda_per_capita_2010_censo,
	'idhm_2010_censo', m.idhm_2010_censo,
	'idhm_renda_2010_censo', m.idhm_renda_2010_censo,
	'idhm_educacao_2010_censo', m.idhm_educacao_2010_censo,
	'idhm_longevidade_2010_censo', m.idhm_longevidade_2010_censo,
	'tx_freq_liq_ens_fund_2010', m.tx_freq_liq_ens_fund_2010,
	'tx_evasao_rede_publica_fund_2014', m.tx_evasao_rede_publica_fund_2014,
	'tx_atividade_10a14_anos_2010', m.tx_atividade_10a14_anos_2010,
	'tx_atividade_10_anos_ou_mais_2010', m.tx_atividade_10_anos_ou_mais_2010,
	'tx_ocupacao_18_anos_ou_mais_2010', m.tx_ocupacao_18_anos_ou_mais_2010,
	'tx_desocupacao_18_anos_ou_mais_2010', m.tx_desocupacao_18_anos_ou_mais_2010,
	'indice_gini_2010', m.indice_gini_2010,
	'populacao_2010', m.populacao_2010,
	'populacao_10a14_anos_2010', m.populacao_10a14_anos_2010,
	'populacao_5a9_anos_m_2010', m.populacao_5a9_anos_m_2010,
	'populacao_5a9_anos_f_2010', m.populacao_5a9_anos_f_2010 
) as municipio
	
from public.municipio_antropometria_{year}_5_10_anos ma 
join  municipio m on m.municipio = ma.municipio)t
""")
    connection.close_connection()
    return municipios


def get_municipios_with_data_forGeoJSON(year=2015):
    connection = get_connection()
    municipios = connection.execute_get_sql(f"""
    select jsonb_agg(t) from (
    select json_build_object (
    'municipio', m.municipio,
	'name', m."name",
	'tx_registros_m', case when ma.total_registros_m > 0 then round(cast((ma.total_registros_m::float / ma.total_registros::float) * 100 as numeric), 2) else 0 end,
	'tx_registros_f', case when ma.total_registros_f > 0 then round(cast((ma.total_registros_f::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_magreza', case when ma.magreza_m > 0 then round(cast((ma.magreza_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_eutrofia', case when ma.eutrofia_m > 0 then round(cast((ma.eutrofia_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_risco_sobrepeso', case when ma.risco_sobrepeso_m > 0 then round(cast((ma.risco_sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_sobrepeso', case when ma.sobrepeso_m > 0 then round(cast((ma.sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_obesidade', case when ma.obesidade_obesidade_grave_m > 0 then round(cast((ma.obesidade_obesidade_grave_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_magreza', case when ma.magreza_f > 0 then round(cast((ma.magreza_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_eutrofia', case when ma.eutrofia_f > 0 then round(cast((ma.eutrofia_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_risco_sobrepeso', case when ma.risco_sobrepeso_f > 0 then round(cast((ma.risco_sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_sobrepeso', case when ma.sobrepeso_f > 0 then round(cast((ma.sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_obesidade', case when ma.obesidade_obesidade_grave_f > 0 then round(cast((ma.obesidade_obesidade_grave_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'total_registros', ma.total_registros,
	'total_registros_m', ma.total_registros_m,
	'total_registros_f', ma.total_registros_f,
	'magreza_m', ma.magreza_m,
	'eutrofia_m', ma.eutrofia_m,
	'risco_sobrepeso_m', ma.risco_sobrepeso_m,
	'sobrepeso_m', ma.sobrepeso_m,
	'obesidade_obesidade_grave_m', ma.obesidade_obesidade_grave_m,
	'magreza_f', ma.magreza_f,
	'eutrofia_f', ma.eutrofia_f,
	'risco_sobrepeso_f', ma.risco_sobrepeso_f,
	'sobrepeso_f', ma.sobrepeso_f,
	'obesidade_obesidade_grave_f', ma.obesidade_obesidade_grave_f
) as municipio

from public.municipio_antropometria_{year}_5_10_anos ma 
join  municipio m on m.municipio = ma.municipio)t
""")
    connection.close_connection()
    return municipios

def get_municipio_with_data(year=2015, ibge=-1):
    connection = get_connection()
    municipios = connection.execute_get_sql(f"""
    select 
json_build_object (
    'municipio', m.municipio,
	'name', m."name",
	'tx_registros_m', case when ma.total_registros_m > 0 then round(cast((ma.total_registros_m::float / ma.total_registros::float) * 100 as numeric), 2) else 0 end,
	'tx_registros_f', case when ma.total_registros_f > 0 then round(cast((ma.total_registros_f::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_magreza', case when ma.magreza_m > 0 then round(cast((ma.magreza_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_eutrofia', case when ma.eutrofia_m > 0 then round(cast((ma.eutrofia_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_risco_sobrepeso', case when ma.risco_sobrepeso_m > 0 then round(cast((ma.risco_sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_sobrepeso', case when ma.sobrepeso_m > 0 then round(cast((ma.sobrepeso_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_m_obesidade', case when ma.obesidade_obesidade_grave_m > 0 then round(cast((ma.obesidade_obesidade_grave_m ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_magreza', case when ma.magreza_f > 0 then round(cast((ma.magreza_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_eutrofia', case when ma.eutrofia_f > 0 then round(cast((ma.eutrofia_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_risco_sobrepeso', case when ma.risco_sobrepeso_f > 0 then round(cast((ma.risco_sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_sobrepeso', case when ma.sobrepeso_f > 0 then round(cast((ma.sobrepeso_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'tx_registros_f_obesidade', case when ma.obesidade_obesidade_grave_f > 0 then round(cast((ma.obesidade_obesidade_grave_f ::float / ma.total_registros::float) * 100 as numeric), 2)  else 0  end,
	'total_registros', ma.total_registros,
	'total_registros_m', ma.total_registros_m,
	'total_registros_f', ma.total_registros_f,
	'magreza_m', ma.magreza_m,
	'eutrofia_m', ma.eutrofia_m,
	'risco_sobrepeso_m', ma.risco_sobrepeso_m,
	'sobrepeso_m', ma.sobrepeso_m,
	'obesidade_obesidade_grave_m', ma.obesidade_obesidade_grave_m,
	'magreza_f', ma.magreza_f,
	'eutrofia_f', ma.eutrofia_f,
	'risco_sobrepeso_f', ma.risco_sobrepeso_f,
	'sobrepeso_f', ma.sobrepeso_f,
	'obesidade_obesidade_grave_f', ma.obesidade_obesidade_grave_f
) as municipio

from public.municipio_antropometria_{year}_5_10_anos ma 
join  municipio m on m.municipio = ma.municipio
where m.municipio = {ibge}

""")
    connection.close_connection()
    return municipios

def get_municipio_identificar(ibge=-1):
    connection = get_connection()

    municipio = connection.execute_get_sql(f"""
        select 
json_build_object (
    'municipio', m.municipio,
	'name', m."name",
	'uf', m.uf_code,
	'microregion_name', m.microregion_name,
	'mesoregion_name', m.mesoregion_name,
	'renda_per_capita_2010_censo', m.renda_per_capita_2010_censo,
	'idhm_2010_censo', m.idhm_2010_censo, 
	'idhm_2010_censo', m.idhm_2010_censo, 
	'idhm_renda_2010_censo', m.idhm_renda_2010_censo, 
	'idhm_educacao_2010_censo', m.idhm_educacao_2010_censo, 
	'idhm_longevidade_2010_censo', m.idhm_longevidade_2010_censo, 
	'tx_freq_liq_ens_fund_2010', m.tx_freq_liq_ens_fund_2010, 
	'tx_evasao_rede_publica_fund_2014', m.tx_evasao_rede_publica_fund_2014, 
	'tx_atividade_10a14_anos_2010', m.tx_atividade_10a14_anos_2010, 
	'tx_atividade_10_anos_ou_mais_2010', m.idhm_2010_censo, 
	'tx_ocupacao_18_anos_ou_mais_2010', m.tx_ocupacao_18_anos_ou_mais_2010, 
	'tx_desocupacao_18_anos_ou_mais_2010', m.tx_desocupacao_18_anos_ou_mais_2010, 
	'indice_gini_2010', m.indice_gini_2010, 
	'populacao_2010', m.populacao_2010, 
	'populacao_10a14_anos_2010', m.populacao_10a14_anos_2010, 
	'populacao_5a9_anos_m_2010', m.populacao_5a9_anos_m_2010, 
	'populacao_5a9_anos_f_2010', m.populacao_5a9_anos_f_2010, 
	'2015', json_build_object (
		'tx_registros_m', case when ma_2015.total_registros_m > 0 then round(cast((ma_2015.total_registros_m::float / ma_2015.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2015.total_registros_f > 0 then round(cast((ma_2015.total_registros_f::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2015.magreza_m > 0 then round(cast((ma_2015.magreza_m ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2015.eutrofia_m > 0 then round(cast((ma_2015.eutrofia_m ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2015.risco_sobrepeso_m > 0 then round(cast((ma_2015.risco_sobrepeso_m ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2015.sobrepeso_m > 0 then round(cast((ma_2015.sobrepeso_m ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2015.obesidade_obesidade_grave_m > 0 then round(cast((ma_2015.obesidade_obesidade_grave_m ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2015.magreza_f > 0 then round(cast((ma_2015.magreza_f ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2015.eutrofia_f > 0 then round(cast((ma_2015.eutrofia_f ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2015.risco_sobrepeso_f > 0 then round(cast((ma_2015.risco_sobrepeso_f ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2015.sobrepeso_f > 0 then round(cast((ma_2015.sobrepeso_f ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2015.obesidade_obesidade_grave_f > 0 then round(cast((ma_2015.obesidade_obesidade_grave_f ::float / ma_2015.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2015.total_registros,
		'total_registros_m', ma_2015.total_registros_m,
		'total_registros_f', ma_2015.total_registros_f,
		'magreza_m', ma_2015.magreza_m,
		'eutrofia_m', ma_2015.eutrofia_m,
		'risco_sobrepeso_m', ma_2015.risco_sobrepeso_m,
		'sobrepeso_m', ma_2015.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2015.obesidade_obesidade_grave_m,
		'magreza_f', ma_2015.magreza_f,
		'eutrofia_f', ma_2015.eutrofia_f,
		'risco_sobrepeso_f', ma_2015.risco_sobrepeso_f,
		'sobrepeso_f', ma_2015.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2015.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2015.pib_x_1000,
		'pib_per_capita', ma_2015.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2015.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2015.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2015.tx_internacao_por_cond_sensiveis_atencao_primaria
	),'2016', json_build_object (
		'tx_registros_m', case when ma_2016.total_registros_m > 0 then round(cast((ma_2016.total_registros_m::float / ma_2016.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2016.total_registros_f > 0 then round(cast((ma_2016.total_registros_f::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2016.magreza_m > 0 then round(cast((ma_2016.magreza_m ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2016.eutrofia_m > 0 then round(cast((ma_2016.eutrofia_m ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2016.risco_sobrepeso_m > 0 then round(cast((ma_2016.risco_sobrepeso_m ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2016.sobrepeso_m > 0 then round(cast((ma_2016.sobrepeso_m ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2016.obesidade_obesidade_grave_m > 0 then round(cast((ma_2016.obesidade_obesidade_grave_m ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2016.magreza_f > 0 then round(cast((ma_2016.magreza_f ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2016.eutrofia_f > 0 then round(cast((ma_2016.eutrofia_f ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2016.risco_sobrepeso_f > 0 then round(cast((ma_2016.risco_sobrepeso_f ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2016.sobrepeso_f > 0 then round(cast((ma_2016.sobrepeso_f ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2016.obesidade_obesidade_grave_f > 0 then round(cast((ma_2016.obesidade_obesidade_grave_f ::float / ma_2016.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2016.total_registros,
		'total_registros_m', ma_2016.total_registros_m,
		'total_registros_f', ma_2016.total_registros_f,
		'magreza_m', ma_2016.magreza_m,
		'eutrofia_m', ma_2016.eutrofia_m,
		'risco_sobrepeso_m', ma_2016.risco_sobrepeso_m,
		'sobrepeso_m', ma_2016.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2016.obesidade_obesidade_grave_m,
		'magreza_f', ma_2016.magreza_f,
		'eutrofia_f', ma_2016.eutrofia_f,
		'risco_sobrepeso_f', ma_2016.risco_sobrepeso_f,
		'sobrepeso_f', ma_2016.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2016.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2016.pib_x_1000,
		'pib_per_capita', ma_2016.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2016.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2016.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2016.tx_internacao_por_cond_sensiveis_atencao_primaria
	),'2017', json_build_object (
		'tx_registros_m', case when ma_2017.total_registros_m > 0 then round(cast((ma_2017.total_registros_m::float / ma_2017.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2017.total_registros_f > 0 then round(cast((ma_2017.total_registros_f::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2017.magreza_m > 0 then round(cast((ma_2017.magreza_m ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2017.eutrofia_m > 0 then round(cast((ma_2017.eutrofia_m ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2017.risco_sobrepeso_m > 0 then round(cast((ma_2017.risco_sobrepeso_m ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2017.sobrepeso_m > 0 then round(cast((ma_2017.sobrepeso_m ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2017.obesidade_obesidade_grave_m > 0 then round(cast((ma_2017.obesidade_obesidade_grave_m ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2017.magreza_f > 0 then round(cast((ma_2017.magreza_f ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2017.eutrofia_f > 0 then round(cast((ma_2017.eutrofia_f ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2017.risco_sobrepeso_f > 0 then round(cast((ma_2017.risco_sobrepeso_f ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2017.sobrepeso_f > 0 then round(cast((ma_2017.sobrepeso_f ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2017.obesidade_obesidade_grave_f > 0 then round(cast((ma_2017.obesidade_obesidade_grave_f ::float / ma_2017.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2017.total_registros,
		'total_registros_m', ma_2017.total_registros_m,
		'total_registros_f', ma_2017.total_registros_f,
		'magreza_m', ma_2017.magreza_m,
		'eutrofia_m', ma_2017.eutrofia_m,
		'risco_sobrepeso_m', ma_2017.risco_sobrepeso_m,
		'sobrepeso_m', ma_2017.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2017.obesidade_obesidade_grave_m,
		'magreza_f', ma_2017.magreza_f,
		'eutrofia_f', ma_2017.eutrofia_f,
		'risco_sobrepeso_f', ma_2017.risco_sobrepeso_f,
		'sobrepeso_f', ma_2017.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2017.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2017.pib_x_1000,
		'pib_per_capita', ma_2017.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2017.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2017.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2017.tx_internacao_por_cond_sensiveis_atencao_primaria
	),'2018', json_build_object (
		'tx_registros_m', case when ma_2018.total_registros_m > 0 then round(cast((ma_2018.total_registros_m::float / ma_2018.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2018.total_registros_f > 0 then round(cast((ma_2018.total_registros_f::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2018.magreza_m > 0 then round(cast((ma_2018.magreza_m ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2018.eutrofia_m > 0 then round(cast((ma_2018.eutrofia_m ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2018.risco_sobrepeso_m > 0 then round(cast((ma_2018.risco_sobrepeso_m ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2018.sobrepeso_m > 0 then round(cast((ma_2018.sobrepeso_m ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2018.obesidade_obesidade_grave_m > 0 then round(cast((ma_2018.obesidade_obesidade_grave_m ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2018.magreza_f > 0 then round(cast((ma_2018.magreza_f ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2018.eutrofia_f > 0 then round(cast((ma_2018.eutrofia_f ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2018.risco_sobrepeso_f > 0 then round(cast((ma_2018.risco_sobrepeso_f ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2018.sobrepeso_f > 0 then round(cast((ma_2018.sobrepeso_f ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2018.obesidade_obesidade_grave_f > 0 then round(cast((ma_2018.obesidade_obesidade_grave_f ::float / ma_2018.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2018.total_registros,
		'total_registros_m', ma_2018.total_registros_m,
		'total_registros_f', ma_2018.total_registros_f,
		'magreza_m', ma_2018.magreza_m,
		'eutrofia_m', ma_2018.eutrofia_m,
		'risco_sobrepeso_m', ma_2018.risco_sobrepeso_m,
		'sobrepeso_m', ma_2018.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2018.obesidade_obesidade_grave_m,
		'magreza_f', ma_2018.magreza_f,
		'eutrofia_f', ma_2018.eutrofia_f,
		'risco_sobrepeso_f', ma_2018.risco_sobrepeso_f,
		'sobrepeso_f', ma_2018.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2018.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2018.pib_x_1000,
		'pib_per_capita', ma_2018.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2018.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2018.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2018.tx_internacao_por_cond_sensiveis_atencao_primaria
	),'2019', json_build_object (
		'tx_registros_m', case when ma_2019.total_registros_m > 0 then round(cast((ma_2019.total_registros_m::float / ma_2019.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2019.total_registros_f > 0 then round(cast((ma_2019.total_registros_f::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2019.magreza_m > 0 then round(cast((ma_2019.magreza_m ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2019.eutrofia_m > 0 then round(cast((ma_2019.eutrofia_m ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2019.risco_sobrepeso_m > 0 then round(cast((ma_2019.risco_sobrepeso_m ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2019.sobrepeso_m > 0 then round(cast((ma_2019.sobrepeso_m ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2019.obesidade_obesidade_grave_m > 0 then round(cast((ma_2019.obesidade_obesidade_grave_m ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2019.magreza_f > 0 then round(cast((ma_2019.magreza_f ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2019.eutrofia_f > 0 then round(cast((ma_2019.eutrofia_f ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2019.risco_sobrepeso_f > 0 then round(cast((ma_2019.risco_sobrepeso_f ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2019.sobrepeso_f > 0 then round(cast((ma_2019.sobrepeso_f ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2019.obesidade_obesidade_grave_f > 0 then round(cast((ma_2019.obesidade_obesidade_grave_f ::float / ma_2019.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2019.total_registros,
		'total_registros_m', ma_2019.total_registros_m,
		'total_registros_f', ma_2019.total_registros_f,
		'magreza_m', ma_2019.magreza_m,
		'eutrofia_m', ma_2019.eutrofia_m,
		'risco_sobrepeso_m', ma_2019.risco_sobrepeso_m,
		'sobrepeso_m', ma_2019.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2019.obesidade_obesidade_grave_m,
		'magreza_f', ma_2019.magreza_f,
		'eutrofia_f', ma_2019.eutrofia_f,
		'risco_sobrepeso_f', ma_2019.risco_sobrepeso_f,
		'sobrepeso_f', ma_2019.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2019.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2019.pib_x_1000,
		'pib_per_capita', ma_2019.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2019.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2019.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2019.tx_internacao_por_cond_sensiveis_atencao_primaria
	),'2020', json_build_object (
		'tx_registros_m', case when ma_2020.total_registros_m > 0 then round(cast((ma_2020.total_registros_m::float / ma_2020.total_registros::float) * 100 as numeric), 2) else 0 end,
		'tx_registros_f', case when ma_2020.total_registros_f > 0 then round(cast((ma_2020.total_registros_f::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_magreza', case when ma_2020.magreza_m > 0 then round(cast((ma_2020.magreza_m ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_eutrofia', case when ma_2020.eutrofia_m > 0 then round(cast((ma_2020.eutrofia_m ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_risco_sobrepeso', case when ma_2020.risco_sobrepeso_m > 0 then round(cast((ma_2020.risco_sobrepeso_m ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_sobrepeso', case when ma_2020.sobrepeso_m > 0 then round(cast((ma_2020.sobrepeso_m ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_m_obesidade', case when ma_2020.obesidade_obesidade_grave_m > 0 then round(cast((ma_2020.obesidade_obesidade_grave_m ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_magreza', case when ma_2020.magreza_f > 0 then round(cast((ma_2020.magreza_f ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_eutrofia', case when ma_2020.eutrofia_f > 0 then round(cast((ma_2020.eutrofia_f ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_risco_sobrepeso', case when ma_2020.risco_sobrepeso_f > 0 then round(cast((ma_2020.risco_sobrepeso_f ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_sobrepeso', case when ma_2020.sobrepeso_f > 0 then round(cast((ma_2020.sobrepeso_f ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'tx_registros_f_obesidade', case when ma_2020.obesidade_obesidade_grave_f > 0 then round(cast((ma_2020.obesidade_obesidade_grave_f ::float / ma_2020.total_registros::float) * 100 as numeric), 2)  else 0  end,
		'total_registros', ma_2020.total_registros,
		'total_registros_m', ma_2020.total_registros_m,
		'total_registros_f', ma_2020.total_registros_f,
		'magreza_m', ma_2020.magreza_m,
		'eutrofia_m', ma_2020.eutrofia_m,
		'risco_sobrepeso_m', ma_2020.risco_sobrepeso_m,
		'sobrepeso_m', ma_2020.sobrepeso_m,
		'obesidade_obesidade_grave_m', ma_2020.obesidade_obesidade_grave_m,
		'magreza_f', ma_2020.magreza_f,
		'eutrofia_f', ma_2020.eutrofia_f,
		'risco_sobrepeso_f', ma_2020.risco_sobrepeso_f,
		'sobrepeso_f', ma_2020.sobrepeso_f,
		'obesidade_obesidade_grave_f', ma_2020.obesidade_obesidade_grave_f,
		'pib_x_1000', ma_2020.pib_x_1000,
		'pib_per_capita', ma_2020.pib_per_capita,
		'tx_bruta_mortalidade_infantil', ma_2020.tx_bruta_mortalidade_infantil,
		'tx_internacao_doencas_saneam_amb_inadequado', ma_2020.tx_internacao_doencas_saneam_amb_inadequado,
		'tx_internacao_por_cond_sensiveis_atencao_primaria', ma_2020.tx_internacao_por_cond_sensiveis_atencao_primaria
	)
) as municipio

from municipio m
join public.municipio_antropometria_2015_5_10_anos ma_2015 on m.municipio = ma_2015.municipio
join public.municipio_antropometria_2016_5_10_anos ma_2016 on m.municipio = ma_2016.municipio
join public.municipio_antropometria_2017_5_10_anos ma_2017 on m.municipio = ma_2017.municipio
join public.municipio_antropometria_2018_5_10_anos ma_2018 on m.municipio = ma_2018.municipio
join public.municipio_antropometria_2019_5_10_anos ma_2019 on m.municipio = ma_2019.municipio
join public.municipio_antropometria_2020_5_10_anos ma_2020 on m.municipio = ma_2020.municipio
where m.municipio = {ibge}
""")
    connection.close_connection()

    return municipio

def executor_in_all_municipios_strategy(municipios, thread_name, year):
    connection = get_connection()
    for municipio in municipios:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(current_time, ' - municipio:', municipio[0], ' - Ano:', year, ' - in ', thread_name)
        subCode = str(municipio[0])[:6]
        connection.execute_sql(f"""-- Masculino MAGREZA de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_m = total_registros_m + soma.total, 
        magreza_m = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'M'
        and imc_x_idade LIKE '%MAGREZA%'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and magreza_m = 0;
    
    -- Masculino EUTROFIA de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_m = total_registros_m + soma.total, 
        eutrofia_m = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'M'
        and imc_x_idade = 'EUTROFIA'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and eutrofia_m = 0;
    
    -- Masculino RISCO DE SOBREPESO/SOBREPESO de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_m = total_registros_m + soma.total, 
        risco_sobrepeso_m = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'M'
        and imc_x_idade = 'RISCO DE SOBREPESO/SOBREPESO'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and risco_sobrepeso_m = 0;
    
    -- Masculino SOBREPESO/OBESIDADE de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_m = total_registros_m + soma.total, 
        sobrepeso_m = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'M'
        and imc_x_idade = 'SOBREPESO/OBESIDADE'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and sobrepeso_m = 0;
    
    -- Masculino OBESIDADE/OBESIDADE GRAVE de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_m = total_registros_m + soma.total, 
        obesidade_obesidade_grave_m = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'M'
        and imc_x_idade = 'OBESIDADE/OBESIDADE GRAVE'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and obesidade_obesidade_grave_m = 0;
    
    
    -- Feminino MAGREZA de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_f = total_registros_f + soma.total, 
        magreza_f = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'F'
        and imc_x_idade LIKE '%MAGREZA%'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and magreza_f = 0;
    
    -- Feminino EUTROFIA de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_f = total_registros_f + soma.total, 
        eutrofia_f = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'F'
        and imc_x_idade = 'EUTROFIA'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and eutrofia_f = 0;
    
    -- Feminino RISCO DE SOBREPESO/SOBREPESO de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_f = total_registros_f + soma.total, 
        risco_sobrepeso_f = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'F'
        and imc_x_idade = 'RISCO DE SOBREPESO/SOBREPESO'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and risco_sobrepeso_f = 0;
    
    -- Feminino SOBREPESO/OBESIDADE de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_f = total_registros_f + soma.total, 
        sobrepeso_f = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'F'
        and imc_x_idade = 'SOBREPESO/OBESIDADE'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and sobrepeso_f = 0;
    
    -- Feminino OBESIDADE/OBESIDADE GRAVE de 10 a 15 anos
    update "municipio_antropometria_{year}_5_10_anos" 
    set total_registros = total_registros + soma.total, 
        total_registros_f = total_registros_f + soma.total, 
        obesidade_obesidade_grave_f = soma.total
    from (
        select coalesce(sum(1), 0) as total
        from dados_brutos.estado_nutricional_acompanhamento_{year}
        where ibge_municipio_acomp = {subCode} -- Variável para ser alterada no Python
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) > 4
        and DATE_PART('year',age(dt_de_acomp, dt_nascimento::date)) < 10
        and sexo = 'F'
        and imc_x_idade = 'OBESIDADE/OBESIDADE GRAVE'
    ) as soma
    WHERE municipio::varchar like '{subCode}%' and obesidade_obesidade_grave_f = 0;
    
    """)
    connection.close_connection()


def split_municipios_count_is_zero(year="2015"):
    municipios = get_all_municipios_by_year_count_is_zero(year)
    splited = np.array_split(municipios, 10)
    return splited


def count_municipios_threading():
    year = "2020"
    municipios_splited = split_municipios_count_is_zero(year)
    for i in range(len(municipios_splited)):
        t = Thread(target=executor_in_all_municipios_strategy, args=(municipios_splited[i], f'Thread {str(i)}', year))
        t.start()

# executor_in_all_municipios_strategy()
# count_municipios_threading()
