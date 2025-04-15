import pandas as pd
import logging
from typing import Dict, Optional
from dbf_processor import DBFProcessor
from db_query import DBQuery
from comparator import ResultComparator
import os


class DBFMerger:
    def __init__(self):
        self.dbf_processor = DBFProcessor()
        self.comparator = ResultComparator()
        self.logger = logging.getLogger('DBFMerger')
        self.should_stop = False

    def stop(self):
        self.should_stop = True

    def process_dbf(self, input_dir: str) -> Optional[pd.DataFrame]:
        try:
            return self.dbf_processor.merge_dbf_files(input_dir)
        except Exception as e:
            self.logger.error(f"Ошибка обработки DBF: {str(e)}")
            return None

    def process_db_queries(self, dbf_df: pd.DataFrame, db_params: Dict) -> Optional[pd.DataFrame]:
        if self.should_stop:
            return None

        try:
            pairs = self.dbf_processor.extract_spn_dato_pairs(dbf_df)
            if not pairs:
                return pd.DataFrame()

            db_results = []
            with DBQuery(
                    host=db_params['host'],
                    user=db_params['user'],
                    password=db_params['password'],
                    database=db_params['database']
            ) as db_query:

                for pair in pairs:
                    if self.should_stop:
                        break

                    query = db_params['sql_query'].replace('{SPN}', str(pair['SPN'])).replace('{DATO}',
                                                                                              str(pair['DATO']))
                    result = db_query.execute_query(query)
                    if result:
                        for row in result:
                            row['SN'] = pair['SN']
                            db_results.append(row)

            return pd.DataFrame(db_results) if db_results else pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Ошибка выполнения запросов: {str(e)}")
            return None

    def compare_and_save(self, dbf_df: pd.DataFrame, db_df: pd.DataFrame, output_path: str) -> bool:
        try:
            comparison = self.comparator.compare_results(dbf_df, db_df)
            self.comparator.save_comparison(comparison, output_path)
            return True
        except Exception as e:
            self.logger.error(f"Ошибка сравнения: {str(e)}")
            return False