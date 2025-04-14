import pandas as pd
import logging
from typing import Dict, List
from openpyxl.styles import PatternFill
from openpyxl import Workbook


class ResultComparator:
    def __init__(self):
        self.logger = logging.getLogger('ResultComparator')
        self.highlight_fill = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')

    def compare_results(self, dbf_df: pd.DataFrame, db_df: pd.DataFrame) -> pd.DataFrame:
        """Сравнивает результаты и возвращает объединенный DataFrame"""
        comparison_data = []

        for _, dbf_row in dbf_df.iterrows():
            sn = dbf_row.get('SN', '')
            db_rows = db_df[db_df['SN'] == sn]

            comparison_data.append({
                'source': 'DBF',
                **dbf_row.to_dict()
            })

            for _, db_row in db_rows.iterrows():
                comparison_data.append({
                    'source': 'DB',
                    **db_row.to_dict()
                })

        return pd.DataFrame(comparison_data)

    def save_comparison(self, df: pd.DataFrame, output_path: str):
        """Сохраняет сравнение с подсветкой различий"""
        writer = pd.ExcelWriter(output_path, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='Comparison')

        workbook = writer.book
        worksheet = writer.sheets['Comparison']

        # Подсветка различий
        for row_idx in range(2, len(df) + 2):
            if row_idx % 2 == 0:  # Строки DBF
                dbf_row = df.iloc[row_idx - 2]
                if row_idx < len(df) + 1 and df.iloc[row_idx - 1]['source'] == 'DB':
                    db_row = df.iloc[row_idx - 1]
                    for col_idx, col in enumerate(df.columns, 1):
                        if col in dbf_row and col in db_row and dbf_row[col] != db_row[col]:
                            worksheet.cell(row=row_idx, column=col_idx).fill = self.highlight_fill
                            worksheet.cell(row=row_idx + 1, column=col_idx).fill = self.highlight_fill

        writer.close()