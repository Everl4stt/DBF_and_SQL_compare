import re
from typing import Dict, List, Optional
import pandas as pd
import logging


class QueryBuilder:
    def __init__(self):
        self.logger = logging.getLogger('QueryBuilder')
        self.placeholder_pattern = re.compile(r'\{(\w+)_(\w+\.dbf)\}', re.IGNORECASE)

    def extract_placeholders(self, query: str) -> List[tuple]:
        """Извлекает плейсхолдеры вида {SPN_P07098.DBF} из запроса"""
        return self.placeholder_pattern.findall(query)

    def build_queries(self, query_template: str, df: pd.DataFrame) -> Dict[str, List[dict]]:
        """
        Генерирует конкретные запросы на основе шаблона и данных

        Args:
            query_template: Шаблон запроса с плейсхолдерами
            df: DataFrame с данными из DBF

        Returns:
            Словарь с SN в качестве ключа и списком параметров для запроса
        """
        placeholders = self.extract_placeholders(query_template)
        if not placeholders:
            return {}

        queries = {}

        # Группируем данные по SN
        grouped = df.groupby('SN')

        for sn, group in grouped:
            params = {}
            valid = True

            for field, filename in placeholders:
                col_name = f"{field}_{filename}"

                if col_name not in group['Field'].values:
                    self.logger.warning(f"Не найдена колонка {col_name} для SN {sn}")
                    valid = False
                    break

                # Берем первое значение для данного SN
                value = group[group['Field'] == col_name]['Value'].iloc[0]
                params[f"{field}_{filename}"] = value

            if valid and params:
                # Форматируем запрос
                try:
                    query = query_template.format(**params)
                    queries[sn] = {
                        'query': query,
                        'params': params,
                        'original_sn': sn
                    }
                except KeyError as e:
                    self.logger.error(f"Ошибка подстановки параметров для SN {sn}: {str(e)}")

        return queries