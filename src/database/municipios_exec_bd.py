from Connection import Connection
import numpy as np
from threading import Thread
from datetime import datetime

def get_connection():
    return Connection("localhost", 5432, "postgres", "postgres", "sldak47")

def get_all_municipios_by_year_count_is_zero(year="2015"):
    connection = get_connection()
    municipios = connection.execute_get_sql(f"select m.municipio from public.municipio_antropometria_{year}_5_10_anos m where m.total_registros = 0")
    connection.close_connection()
    return municipios

def get_all_municipios():
    connection = get_connection()
    municipios = connection.execute_get_sql("select m.municipio from public.municipio m order by m.municipio ")
    connection.close_connection()
    return municipios

def executor_in_all_municipios_strategy(municipios, thread_name, year):
    connection = get_connection()
    for municipio in municipios:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(current_time, ' - municipio:', municipio[0], ' - Ano:', year , ' - in ', thread_name)
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
#count_municipios_threading()
