import pymysql
from pymysql import Error
import logging
from typing import List, Dict, Optional


class DBQuery:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.connection_params = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.connection = None
        self.should_stop = False
        self.logger = logging.getLogger('DBQuery')

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> bool:
        try:
            self.connection = pymysql.connect(**self.connection_params)
            return True
        except Error as e:
            self.logger.error(f"Ошибка подключения: {str(e)}")
            return False

    def disconnect(self):
        if self.connection and self.connection.open:
            self.connection.close()

    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        if self.should_stop:
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Error as e:
            self.logger.error(f"Ошибка выполнения запроса: {str(e)}")
            return None

    def stop(self):
        self.should_stop = True