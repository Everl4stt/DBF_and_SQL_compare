import pymysql
from pymysql import Error
from typing import List, Dict, Optional, Union
import logging


class DBQuery:
    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Инициализация подключения к MariaDB

        :param host: Хост БД
        :param user: Пользователь БД
        :param password: Пароль
        :param database: Имя базы данных
        """
        self.connection_params = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.connection = None
        self.logger = logging.getLogger('DBQuery')

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Поддержка контекстного менеджера"""
        self.disconnect()

    def connect(self) -> bool:
        """Устанавливает соединение с базой данных"""
        try:
            self.connection = pymysql.connect(**self.connection_params)
            self.logger.info("Успешное подключение к базе данных")
            return True
        except Error as e:
            self.logger.error(f"Ошибка подключения к базе данных: {str(e)}")
            return False

    def disconnect(self):
        """Закрывает соединение с базой данных"""
        if self.connection and self.connection.open:
            self.connection.close()
            self.logger.info("Соединение с базой данных закрыто")

    def execute_query(self, query: str, params: Union[tuple, Dict] = None) -> Optional[List[Dict]]:
        """
        Выполняет SQL запрос с параметрами

        :param query: SQL запрос с плейсхолдерами %s
        :param params: Параметры для подстановки в запрос
        :return: Список словарей с результатами или None при ошибке
        """
        if not self.connection or not self.connection.open:
            if not self.connect():
                return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                self.logger.debug(f"Выполнен запрос: {query} с параметрами {params}")
                return result
        except Error as e:
            self.logger.error(f"Ошибка выполнения запроса {query}: {str(e)}")
            return None

    def batch_execute(self, query: str, params_list: List[Union[tuple, Dict]]) -> Dict[str, List[Dict]]:
        """
        Выполняет один запрос для нескольких наборов параметров

        :param query: SQL запрос
        :param params_list: Список параметров
        :return: Словарь с результатами
        """
        results = {}
        for params in params_list:
            if params:
                key = str(params[0]) if isinstance(params, (tuple, list)) else next(iter(params.values()))
                result = self.execute_query(query, params)
                if result:
                    results[key] = result
        return results