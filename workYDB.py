import os
import ydb
import ydb.iam


driver = ydb.Driver(
  endpoint=os.getenv('YDB_ENDPOINT'),
  database=os.getenv('YDB_DATABASE'),
  credentials=ydb.iam.MetadataUrlCredentials(),)

# Wait for the driver to become active for requests.G
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

def truncate_string(string, max_length):
    if len(string.encode('utf-8')) > max_length:
        return string[:max_length]
    else:
        return string
    
class Ydb:
    def replace_query(self, tableName: str, rows: dict):
        #print('попали в инсерт')
        field_names = rows.keys()
        fields_format = ", ".join(field_names)
        my_list = list(rows.values())
    
        value = '('
        for i in my_list:
            if i == 'UF_CRM_1675796663' or i == 'UF_CRM_1675787858':
                i='None'
            try:
                i = i.replace('"',"'")    
            except:
                1 + 0

            i = truncate_string(str(i), 2000)            
            value += f'"{i}",'
            
        value = value[:-1] + ')'
        # values_placeholder_format = ', '.join(my_list)
        query = f"REPLACE INTO {tableName} ({fields_format}) VALUES {value}"
        #print(query)

        def a(session):
            session.transaction(ydb.SerializableReadWrite()).execute(
            #session(ydb.SerializableReadWrite()).execute(
                query,
                commit_tx=True,
            )
        return pool.retry_operation_sync(a)

    def update_query(self, tableName: str, rows: dict, where: str):
        # 'where id > 20 '
        field_names = rows.keys()
        fields_format = ", ".join(field_names)
        my_list = list(rows.values())
        sets = ''
        for key, value in rows.items():
            if key == 'ID':
                continue
            sets += f'{key} = "{value}",'
            """
            try:
                sets += f'{key} = {int(value)},'
                
            except Exception as e:
                print(e)
                sets += f"{key} = '{value}',"
            """
        sets = sets[:-1]

        # values_placeholder_format = ', '.join(my_list)
        query = f'UPDATE {tableName} SET {sets} WHERE {where}'
        # query = f"INSERT INTO {tableName} ({fields_format}) " \
        print(query)

        def a(session):
            session.transaction(ydb.SerializableReadWrite()).execute(
                query,
                commit_tx=True,
            )
        return pool.retry_operation_sync(a)

    def delete_query(self, tableName: str, where: str):
        # 'where id > 20 '
        query = f'DELETE FROM {tableName} WHERE {where}'
        print(query)

        def a(session):
            session.transaction(ydb.SerializableReadWrite()).execute(
                query,
                commit_tx=True,
            )
        return pool.retry_operation_sync(a)

    def create_table(self, tableName: str, fields: dict):
        query = f'CREATE TABLE {tableName} ('
        for key, value in fields.items():
            query += f'{key} String,'

        query = query[:-1] + ', PRIMARY KEY (ID) ) '
        print('CREATE TABLE',tableName)
        def a(session):
            session.execute_scheme(
                query,
                )
        return pool.retry_operation_sync(a)
    
    def get_last_date_modify(self, tableName: str):
        """Внимание запро стоит доваольно много для 2000 строк 2070 RU"""
        query = f"SELECT MAX(DATE_MODIFY) FROM {tableName};"
        print(query)
        def a(session):
            return session.transaction().execute(
                query,
                commit_tx=True,
            )
        b = pool.retry_operation_sync(a)
        print('b',b)
        rez = b[0].rows[0]['column0'].decode('utf-8')
        print('date_modify',rez)
        return rez

    def select_query(self,tableName: str, dealID: str):
        # 'where id > 20 '
        query = f'SELECT * FROM {tableName} WHERE ID="{dealID}"'
        print(query)

        def a(session):
            return session.transaction().execute(
                query,
                commit_tx=True,
            )
        b = pool.retry_operation_sync(a)
        # string = b_string.decode('utf-8')
        # IndexError: list index out of range если нет данныйх
        print('b',b)
        rez = b[0].rows
        print('rez',rez)
        return rez


def handler(event, context):
    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }