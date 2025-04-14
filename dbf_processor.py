import os
from dbfread import DBF
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import logging


class DBFProcessor:
    def __init__(self):
        self.logger = logging.getLogger('DBFProcessor')

    @staticmethod
    def validate_directory(input_dir: str) -> bool:
        """Проверяет существует ли директория и содержит ли DBF файлы"""
        if not os.path.isdir(input_dir):
            raise ValueError(f"Директория не существует: {input_dir}")

        dbf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.dbf')]
        if not dbf_files:
            raise ValueError(f"В директории нет DBF файлов: {input_dir}")
        return True

    def process_to_dataframe(self, input_dir: str) -> pd.DataFrame:
        """
        Обрабатывает DBF файлы и возвращает DataFrame

        :param input_dir: Путь к директории с DBF
        :return: DataFrame с результатами
        """
        self.validate_directory(input_dir)
        unified_data = []

        for filename in os.listdir(input_dir):
            if not filename.lower().endswith('.dbf'):
                continue

            filepath = os.path.join(input_dir, filename)
            try:
                table = DBF(filepath, encoding='cp866')
                for record in table:
                    sn = record.get('SN')
                    if sn is None:
                        continue

                    for field, value in record.items():
                        if field != 'SN':
                            unified_data.append({
                                'SN': str(sn),
                                'Source': filename,
                                'Field': field,
                                'Value': str(value) if value is not None else ''
                            })
            except Exception as e:
                self.logger.error(f"Ошибка обработки файла {filename}: {str(e)}")
                continue

        return pd.DataFrame(unified_data)

    def extract_unique_values(self, df: pd.DataFrame, field_names: List[str]) -> List[str]:
        """
        Извлекает уникальные значения указанных полей

        :param df: DataFrame с данными
        :param field_names: Список имен полей
        :return: Список уникальных значений
        """
        if df.empty:
            return []

        return df[df['Field'].isin(field_names)]['Value'].unique().tolist()