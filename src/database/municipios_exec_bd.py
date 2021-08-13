from Connection import Connection


def get_all_municipios():
    connection = Connection("localhost", 5432, "postgres", "postgres", "sldak47");
    municipios = connection.execute_sql("select m.municipio from public.municipio m limit 10 ")
    return municipios


def executor_in_all_municipios_strategy():
    municipios = get_all_municipios()
    connection = Connection("localhost", 5432, "postgres", "postgres", "sldak47");
    for municipio in municipios:
        print(connection.execute_sql("select * from public.municipio m where m.municipio = " + str(municipio[0])))
    connection.close_connection()


executor_in_all_municipios_strategy()
