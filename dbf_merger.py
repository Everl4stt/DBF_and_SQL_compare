import pandas as pd
import logging
import os
from typing import Dict, Optional, Callable
from dbf_processor import DBFProcessor
from db_query import DBQuery
from comparator import ResultComparator


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
            dbf_df = self.dbf_processor.merge_dbf_files(input_dir)
            self._save_to_excel(dbf_df, "dbf_merged.xlsx", "Merged_DBF")
            return dbf_df
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

            total = len(pairs)
            db_results = []
            progress_callback = db_params.get('progress_callback')

            with DBQuery(
                    host=db_params['host'],
                    user=db_params['user'],
                    password=db_params['password'],
                    database=db_params['database']
            ) as db_query:

                for i, pair in enumerate(pairs, 1):
                    if self.should_stop:
                        break

                    if progress_callback:
                        progress_callback(i, total)

                    query = db_params['sql_query'].replace('{SPN}', str(pair['SPN'])).replace('{DATO}',
                                                                                              str(pair['DATO']))
                    result = db_query.execute_query(query)
                    if result:
                        for row in result:
                            row['SN'] = pair['SN']
                            row['source_file'] = pair['source_file']
                            db_results.append(row)

            db_df = pd.DataFrame(db_results) if db_results else pd.DataFrame()
            self._save_to_excel(db_df, "db_results.xlsx", "DB_Results")
            return db_df

        except Exception as e:
            self.logger.error(f"Ошибка выполнения запросов: {str(e)}")
            return None

    def compare_and_save(self, dbf_df: pd.DataFrame, db_df: pd.DataFrame, output_path: str) -> bool:
        """Сравнивает и сохраняет результаты в отдельный файл"""
        try:
            comparison = self.comparator.compare_results(dbf_df, db_df)
            self.comparator.save_comparison(comparison, output_path)
            return True
        except Exception as e:
            self.logger.error(f"Ошибка сравнения: {str(e)}")
            return False

    def _save_to_excel(self, df: pd.DataFrame, filename: str, sheet_name: str):
        """Сохраняет DataFrame в Excel файл"""
        try:
            os.makedirs("results", exist_ok=True)
            filepath = os.path.join("results", filename)
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            self.logger.info(f"Файл сохранен: {filepath}")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения Excel: {str(e)}")
            raise