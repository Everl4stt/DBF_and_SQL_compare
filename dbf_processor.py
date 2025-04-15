import os
from dbfread import DBF
import pandas as pd
import logging
from typing import Dict, List, Tuple
from collections import defaultdict


class DBFProcessor:
    def __init__(self):
        self.logger = logging.getLogger('DBFProcessor')

    def merge_dbf_files(self, input_dir: str) -> pd.DataFrame:
        """Объединяет DBF файлы по полю SN с дублированием строк"""
        if not os.path.isdir(input_dir):
            raise ValueError(f"Директория не существует: {input_dir}")

        # Получаем все DBF файлы, исключая начинающиеся на D
        dbf_files = [
            f for f in os.listdir(input_dir)
            if f.lower().endswith('.dbf') and not f.upper().startswith('D')
        ]

        if not dbf_files:
            raise ValueError("Не найдено подходящих DBF файлов")

        # Собираем данные из всех файлов, группируя по SN
        sn_data = defaultdict(list)
        file_data = {}

        for filename in dbf_files:
            filepath = os.path.join(input_dir, filename)
            try:
                table = list(DBF(filepath, encoding='cp866'))
                file_data[filename] = table
                for record in table:
                    sn = record.get('SN')
                    if sn is not None:
                        sn_data[sn].append((filename, dict(record)))
            except Exception as e:
                self.logger.error(f"Ошибка чтения файла {filename}: {str(e)}")
                continue

        if not sn_data:
            raise ValueError("Нет данных для объединения")

        # Создаем объединенные записи с дублированием
        merged_data = []

        for sn, records in sn_data.items():
            # Группируем записи по файлам
            files_records = defaultdict(list)
            for filename, record in records:
                files_records[filename].append(record)

            # Получаем максимальное количество записей для этого SN
            max_count = max(len(recs) for recs in files_records.values())

            # Создаем дублированные записи
            for i in range(max_count):
                merged_record = {'SN': sn}
                for filename, records in files_records.items():
                    record = records[i % len(records)]  # Циклически выбираем записи
                    for key, value in record.items():
                        if key != 'SN':
                            merged_record[f"{key}_{filename}"] = value
                merged_data.append(merged_record)

        return pd.DataFrame(merged_data)

    def extract_spn_dato_pairs(self, df: pd.DataFrame) -> List[Dict]:
        """Извлекает пары SPN и DATO из объединенного DataFrame"""
        pairs = []
        for _, row in df.iterrows():
            # Ищем все колонки SPN и DATO в объединенных данных
            spn_cols = [col for col in df.columns if col.startswith('SPN_')]
            dato_cols = [col for col in df.columns if col.startswith('DATO_')]

            for spn_col in spn_cols:
                filename = spn_col[4:]  # Извлекаем имя файла
                dato_col = f"DATO_{filename}"
                if dato_col in df.columns:
                    spn = row[spn_col]
                    dato = row[dato_col]
                    if pd.notna(spn) and pd.notna(dato):
                        pairs.append({
                            'SN': row['SN'],
                            'SPN': spn,
                            'DATO': dato,
                            'source_file': filename
                        })
        return pairs