import sys, psycopg2
import logging
from configparser import ConfigParser


class Db_Connection:
# To config 
    def config(self,filename='./config.ini', section='postgres'):

        parser = ConfigParser()
        parser.read(filename)
        
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
            # print(params)
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db

    def connect(self):
        conn = None
        try:
            params = self.config()
            # if params !={}:
            print('Connecting to the PostgreSQL database...')
            connection = psycopg2.connect(**params)
            # conn.autocommit = True
            cursor = connection.cursor()

            print("success")

        except psycopg2.OperationalError as e:
            logging.error(database, f"db Operational error - {e.pgerror}")
        except psycopg2.Error as e:
            logging.error(database, f"DB general error - {e.pgerror}") 
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(database, f" Unknown error - {format(e)}")
        
        else:
            self._connection = connection
            self._cursor = cursor
            self._counter = 0
            
# To check connection exist
    def _check_connection(self):
        try:
            self._connection

        except AttributeError:
            print('ERROR: NOT Connected to Database')
            sys.exit()

# execute query
    def _execute(self, query, Placeholder_value = None):
        self._check_connection()
        if Placeholder_value == None or None in Placeholder_value:
            self._cursor.execute(query)
            # print( '-# ' + query.as_string(self._connection) + ';\n' )
        else:
            self._cursor.execute(query, Placeholder_value)
            # print( '-# ' + query.as_string(self._connection) % Placeholder_value + ';\n' )

# To Commit Changes
    def commit(self):
        self._check_connection()
        self._connection.commit()
        print('-# COMMIT '+ str(self._counter) +' changes\n')
        self._counter = 0

# To Close connection
    def close(self, commit = False):
        self._check_connection()
        if commit:
            self.commit()
        else:
            self._cursor.close()
            self._connection.close()
        if self._counter > 0:
            print('-# '+ str(self._counter) +' changes NOT commited  CLOSE connection\n')
        else:
            print('-# CLOSE connection\n')

    