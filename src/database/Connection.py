import psycopg2


class Connection:

    def __init__(self, host="localhost", port=5432, database="postgres", user="postgres", password=""):
        try:
            con = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
            self.connection = con
        except Exception as e:
            return e

    def execute_sql(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        self.connection.commit()
        result = cur.fetchall()
        return result

    def close_connection(self):
        cur = self.connection.cursor()
        cur.close()
        self.connection.close()
