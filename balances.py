import psycopg2
from psycopg2.extras import execute_values
from multiprocessing import Pool
from functools import reduce


class Replicator:

    def __init__(self, conn_read: dict, read_table: str, conn_write: dict, write_table: str,
                 query_partition: str, table_partition: str, pool_num=5):
        self.conn_read = conn_read
        self.conn_write = conn_write
        self.read_table = read_table
        self.write_table = write_table
        self.partition_field = table_partition
        self.query_partition = query_partition
        self.pool_num = pool_num

    def run(self):
        table_partition_values = f"select {self.partition_field} from {self.read_table} group by {self.partition_field}"
        total_inserted_rows = []
        for partition_value in self._read_data(table_partition_values):
            query_pool = self._get_boundary(partition_value[0])
            data = self._pool(query_pool)
            write_result = self._save_to_db(data)
            total_inserted_rows.append(write_result)
            print(f'{write_result} has been inserted for partition {partition_value[0]}')
        print(f'Total inserted rows: {reduce(lambda x, y: x+y, total_inserted_rows)}')

    def _get_read_conn(self):

        try:
            conn_string = psycopg2.connect(database=self.conn_read['database'],
                                            user=self.conn_read['user'],
                                            host=self.conn_read['host'],
                                            password=self.conn_read['password'])
            return conn_string
        except ConnectionError as error:
            print(f'Connection error has occurred when reading from source: {error}')

    def _read_data(self, query):
        try:
            with self._get_read_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
            return result

        except Exception as error:
            print(f'Exception has occurred while executing {query}: {error}')

    def _get_boundary(self, table_partition_value):

        boundary_query = f'select min({self.query_partition}), max({self.query_partition}) from {self.read_table} ' \
                         f'where {self.partition_field} = {table_partition_value}'
        boundary_result = self._read_data(boundary_query)
        if type(boundary_result[0][0]) == int and type(boundary_result[0][1]) == int:
            query_pool = []
            min_value = boundary_result[0][0]
            max_value = boundary_result[0][1]
            stride = (max_value - min_value) / self.pool_num
            partition_nr = 1

            while partition_nr <= self.pool_num:
                if partition_nr == 1 and partition_nr < self.pool_num:
                    query_pool.append(
                        f'select * from {self.read_table} where {self.partition_field} = {table_partition_value} '
                        f'and {self.query_partition} < {int(min_value + stride)}')

                elif partition_nr > 1 and partition_nr < self.pool_num:
                    min_value += stride
                    next_stride = min_value + stride
                    query_pool.append(
                        f'select * from {self.read_table} where {self.partition_field} = {table_partition_value} and '
                        f'{self.query_partition} >= {int(min_value)} and {self.query_partition} < {int(next_stride)}')

                elif partition_nr > 1 and partition_nr == self.pool_num:
                    query_pool.append(
                        f'select * from {self.read_table} where {self.partition_field} = {table_partition_value} and '
                        f'{self.query_partition} >= {int(min_value + stride)}')

                partition_nr += 1
            return query_pool
        else:
            raise ValueError('Boundary field must be of type int')

    def _pool(self, query):
        with Pool(self.pool_num) as p:
            data_load = p.map(self._read_data, query)
            return data_load

    def _save_to_db(self, data):
        conn = psycopg2.connect(database=self.conn_write['database'],
                                user=self.conn_write['user'],
                                host=self.conn_write['host'],
                                password=self.conn_write['password'])

        conn.autocommit = True

        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '{self.write_table}' ")
                    result = cursor.fetchall()
                    table_columns = ', '.join([x[0] for x in result])

            with conn:
                inserted_rows = []
                with conn.cursor() as cursor:
                    for portion in data:
                        execute_values(cursor, f'insert into {self.write_table} ({table_columns}) values %s', portion)
                        inserted_rows.append(len(portion))

                sum_rows = reduce(lambda x, y: x+y, inserted_rows)

            conn.close()
            return sum_rows

        except Exception as error:
            print(f'Exception has occurred: {error}')


if __name__ == '__main__':
    conn_read = {
      'user': 'userspark',
      'password': 'mypass',
      'host': '127.0.0.1',
      'database': 'spark'
    }

    conn_write = {
      'user': 'userspark',
      'password': 'mypass',
      'host': '127.0.0.1',
      'database': 'sparkreplica'
    }

    balances_replica = Replicator(conn_read, 'balances', conn_write, 'balances', 'id_row', 'year')
    balances_replica.run()
