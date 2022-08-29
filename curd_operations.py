import psycopg2,sys,logging
import psycopg2.sql as sql
from db_connections import Db_Connection


class Curd_Operation(Db_Connection):
    def __init__(self, schema_name=None, table_name=None,primarykey=None):
        self.schema_name = schema_name
        self.table_name = table_name
        self.primarykey = primarykey

    def create_table(self,file_name):
        self.connect()
        read_query = open(file_name,"r").read()
        self._execute(read_query)
        self.commit()
        self.close("commit")

    def insert(self, **column_value):
        self.connect()
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(self.schema_name),
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.Identifier, column_value.keys())),
            sql.SQL(', ').join(sql.Placeholder() * len(column_value.values()))
        )
        record_to_insert = tuple(column_value.values())
        self._execute(insert_query, record_to_insert)
        self._counter += 1
        self.commit()
        self.close("commit")


    def insert_many(self, columns, rows):
        self.connect()
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(self.schema_name),
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(rows[0]))
        )
        for row in rows:
            row = tuple(row)
            self._execute(insert_query, row)
            self._counter += 1
        self.commit()
        self.close("commit")


    def select(self, columns, primaryKey_value=None):
        self.connect()
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT {} FROM {}.{}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name)
            )
            self._execute(select_query)
            
        else:
            select_query = sql.SQL("SELECT {} FROM {}.{} WHERE {} = {}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute(select_query, (primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
            self.commit()
            self.close("commit")
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print('-# ' + str(selected) + '\n')
            return selected


    def select_all(self, primaryKey_value=None):
        self.connect()
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name))
            self._execute(select_query)
        else:
            select_query = sql.SQL("SELECT * FROM {}.{} WHERE {} = {}").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute(select_query, (primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
            self.commit()
            self.close("commit")
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print('-# ' + str(selected) + '\n')
            return selected
