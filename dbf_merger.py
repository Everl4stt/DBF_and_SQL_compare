import pandas as pd
from typing import Dict, Any, Optional
from dbf_processor import DBFProcessor
from get_db_data import DBQuery
import logging
import os


class DBFMerger:
    def __init__(self):
        self.dbf_processor = DBFProcessor()
        self.logger = logging.getLogger('DBFMerger')

    def save_to_excel(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        Сохраняет DataFrame в Excel

        :param df: Данные для сохранения
        :param output_path: Путь для сохранения
        :return: Статус операции
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_excel(output_path, index=False, engine='openpyxl')
            self.logger.info(f"Файл успешно сохранен: {output_path}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка сохранения файла {output_path}: {str(e)}")
            return False

    def process_dbf_files(self, input_dir: str, output_path: str) -> bool:
        """
        Обрабатывает DBF файлы и сохраняет результат

        :param input_dir: Путь к DBF файлам
        :param output_path: Путь для сохранения
        :return: Статус операции
        """
        try:
            dbf_df = self.dbf_processor.process_to_dataframe(input_dir)
            return self.save_to_excel(dbf_df, output_path)
        except Exception as e:
            self.logger.error(f"Ошибка обработки DBF файлов: {str(e)}")
            return False

    def process_sql_queries(self, db_params: Dict[str, Any], fio_list: list[str], output_path: str) -> bool:
        """
        Выполняет SQL запросы и сохраняет результат

        :param db_params: Параметры подключения
        :param fio_list: Список ФИО для запросов
        :param output_path: Путь для сохранения
        :return: Статус операции
        """
        if not db_params.get('sql_query') or not fio_list:
            self.logger.warning("Нет параметров для SQL запросов")
            return False

        try:
            with DBQuery(
                    host=db_params['host'],
                    user=db_params['user'],
                    password=db_params['password'],
                    database=db_params['database']
            ) as db_query:

                sql_results = []
                for fio in fio_list:
                    result = db_query.execute_query(db_params['sql_query'], (fio,))
                    if result:
                        for row in result:
                            row['FIO'] = fio
                            sql_results.append(row)

                if sql_results:
                    # Преобразуем к той же структуре, что и DBF данные
                    sql_df = pd.DataFrame([
                        {
                            'SN': '',
                            'Source': 'MariaDB',
                            'Field': k,
                            'Value': str(v) if v is not None else ''
                        }
                        for row in sql_results
                        for k, v in row.items()
                    ])
                    return self.save_to_excel(sql_df, output_path)
                return False
        except Exception as e:
            self.logger.error(f"Ошибка выполнения SQL запросов: {str(e)}")
            return False

    def process_all(self, input_dir: str, dbf_output: str, db_output: str, db_params: Dict[str, Any]) -> bool:
        """
        Основной метод обработки

        :param input_dir: Путь к DBF файлам
        :param dbf_output: Путь для DBF результатов
        :param db_output: Путь для SQL результатов
        :param db_params: Параметры БД
        :return: Статус операции
        """
        try:
            # Обработка DBF
            dbf_df = self.dbf_processor.process_to_dataframe(input_dir)
            dbf_success = self.save_to_excel(dbf_df, dbf_output)

            # Обработка SQL
            sql_success = True
            if db_params.get('sql_query'):
                fio_list = self.dbf_processor.extract_unique_values(
                    dbf_df,
                    ['FIO', 'ФИО', 'NAME', 'ИМЯ', 'FAMILIYA', 'ФАМИЛИЯ']
                )
                sql_success = self.process_sql_queries(db_params, fio_list, db_output)

            return dbf_success and (not db_params.get('sql_query') or sql_success)
        except Exception as e:
            self.logger.error(f"Ошибка в процессе обработки: {str(e)}")
            return False