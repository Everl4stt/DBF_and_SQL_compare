import pandas as pd
import logging
import os
from typing import Dict, Any, Optional
from dbf_processor import DBFProcessor
from db_query import DBQuery
from comparator import ResultComparator


class DBFMerger:
    def __init__(self):
        self.dbf_processor = DBFProcessor()
        self.query = None
        self.comparator = ResultComparator()
        self.logger = logging.getLogger('DBFMerger')
        self.should_stop = False

    def stop(self):
        self.should_stop = True
        if self.query:
            self.query.stop()

    def process(self, input_dir: str, db_params: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Основной метод обработки"""
        if self.should_stop:
            return {}

        try:
            # 1. Объединение DBF файлов
            self.logger.info("Объединение DBF файлов...")
            dbf_df = self.dbf_processor.merge_dbf_files(input_dir)

            # 2. Извлечение пар SPN/DATO
            pairs = self.dbf_processor.extract_spn_dato_pairs(dbf_df)

            # 3. Выполнение запросов к БД
            db_results = []
            if db_params.get('sql_query') and pairs:
                self.query = DBQuery(
                    host=db_params['host'],
                    user=db_params['user'],
                    password=db_params['password'],
                    database=db_params['database']
                )

                if not self.query.connect():
                    raise ConnectionError("Не удалось подключиться к базе данных")

                for pair in pairs:
                    if self.should_stop:
                        break

                    query = db_params['sql_query'].replace('{SPN}', str(pair['SPN'])).replace('{DATO}',
                                                                                              str(pair['DATO']))
                    result = self.query.execute_query(query)

                    if result:
                        for row in result:
                            row['SN'] = pair['SN']
                            row['source_file'] = pair['source_file']
                            db_results.append(row)

            db_df = pd.DataFrame(db_results) if db_results else pd.DataFrame()

            return {
                'dbf': dbf_df,
                'db': db_df
            }

        except Exception as e:
            self.logger.error(f"Ошибка обработки: {str(e)}")
            raise
        finally:
            if self.query:
                self.query.disconnect()