import os
from dbfread import DBF
import pandas as pd
import logging
from typing import Tuple, Dict, List
from collections import defaultdict


class DBFProcessor:
    def __init__(self):
        self.logger = logging.getLogger('DBFProcessor')

    def merge_dbf_files(self, input_dir: str) -> Tuple[pd.DataFrame, Dict[str, List[Dict]]]:
        """Объединяет DBF файлы с дублированием строк"""
        if not os.path.isdir(input_dir):
            raise ValueError(f"Директория не существует: {input_dir}")

        file_data = defaultdict(list)
        all_sns = set()

        # Собираем данные из всех файлов
        for filename in os.listdir(input_dir):
            if not filename.lower().endswith('.dbf'):
                continue

            filepath = os.path.join(input_dir, filename)
            try:
                table = DBF(filepath, encoding='cp866')
                for record in table:
                    record_dict = dict(record)
                    sn = record_dict.get('SN', '')
                    record_dict['source_file'] = filename
                    file_data[sn].append(record_dict)
                    all_sns.add(sn)
            except Exception as e:
                self.logger.error(f"Ошибка чтения файла {filename}: {str(e)}")
                continue

        if not file_data:
            raise ValueError("Не найдено ни одного DBF файла с данными")

        # Создаем объединенные записи с дублированием
        merged_data = []
        for sn in all_sns:
            records = file_data[sn]
            max_count = max(len(recs) for recs in file_data.values())

            for i in range(max_count):
                merged_record = {'SN': sn}
                for record in records:
                    filename = record['source_file']
                    for key, value in record.items():
                        if key != 'source_file':
                            merged_record[f"{key}_{filename}"] = value
                merged_data.append(merged_record)

        return pd.DataFrame(merged_data), file_data