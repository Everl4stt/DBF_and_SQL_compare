import os
from dbfread import DBF
import pandas as pd
import logging
from typing import Dict, List


class DBFProcessor:
    def __init__(self):
        self.logger = logging.getLogger('DBFProcessor')

    def merge_dbf_files(self, input_dir: str) -> pd.DataFrame:
        """Объединяет все DBF файлы из указанной директории в один DataFrame"""
        if not os.path.isdir(input_dir):
            raise ValueError(f"Директория не существует: {input_dir}")

        all_data = []

        for filename in os.listdir(input_dir):
            if not filename.lower().endswith('.dbf'):
                continue

            filepath = os.path.join(input_dir, filename)
            try:
                table = DBF(filepath, encoding='cp866')
                for record in table:
                    record['source_file'] = filename
                    all_data.append(record)
            except Exception as e:
                self.logger.error(f"Ошибка чтения файла {filename}: {str(e)}")
                continue

        if not all_data:
            raise ValueError("Не найдено ни одного DBF файла с данными")

        return pd.DataFrame(all_data)

    def extract_spn_dato_pairs(self, df: pd.DataFrame) -> List[Dict]:
        """Извлекает пары SPN и DATO для каждого файла"""
        pairs = []
        files = df['source_file'].unique()

        for file in files:
            file_df = df[df['source_file'] == file]
            for _, row in file_df.iterrows():
                if 'SPN' in row and 'DATO' in row:
                    pairs.append({
                        'SN': row.get('SN', ''),
                        'SPN': row['SPN'],
                        'DATO': row['DATO'],
                        'source_file': file,
                        'original_data': row.to_dict()
                    })

        return pairs