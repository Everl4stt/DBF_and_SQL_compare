import pandas as pd
import logging
from typing import Dict, Optional, Tuple
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

    def process_dbf(self, input_dir: str) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
        try:
            return self.dbf_processor.merge_dbf_files(input_dir)
        except Exception as e:
            self.logger.error(f"Ошибка обработки DBF: {str(e)}")
            return None, None

    def process_db_queries(self, dbf_df: pd.DataFrame, db_params: Dict) -> Optional[pd.DataFrame]:
        if self.should_stop or dbf_df.empty:
            return None

        try:
            # Получаем уникальные пары SPN/DATO
            spn_dato_pairs = set()
            for col in dbf_df.columns:
                if col.startswith('SPN_'):
                    filename = col[4:]
                    dato_col = f"DATO_{filename}"
                    if dato_col in dbf_df.columns:
                        for _, row in dbf_df.iterrows():
                            spn = row[col]
                            dato = row[dato_col]
                            if pd.notna(spn) and pd.notna(dato):
                                spn_dato_pairs.add((spn, dato, filename))

            db_results = []
            with DBQuery(
                    host=db_params['host'],
                    user=db_params['user'],
                    password=db_params['password'],
                    database=db_params['database']
            ) as db_query:

                for spn, dato, filename in spn_dato_pairs:
                    if self.should_stop:
                        break

                    query = db_params['sql_query'].replace('{SPN}', str(spn)).replace('{DATO}', str(dato))
                    result = db_query.execute_query(query)
                    if result:
                        for row in result:
                            row['SN'] = spn
                            row['source_file'] = filename
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