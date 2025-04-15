import pandas as pd
import logging
from openpyxl.styles import PatternFill
from openpyxl import Workbook
from typing import Dict


class ResultComparator:
    def __init__(self):
        self.highlight_fill = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
        self.logger = logging.getLogger('ResultComparator')

    def compare_results(self, dbf_data: pd.DataFrame, db_data: pd.DataFrame) -> pd.DataFrame:
        comparison = []

        for _, dbf_row in dbf_data.iterrows():
            sn = dbf_row.get('SN', '')
            comparison.append({'Source': 'DBF', **dbf_row.to_dict()})

            matching_db_rows = db_data[db_data['SN'] == sn]
            for _, db_row in matching_db_rows.iterrows():
                comparison.append({'Source': 'DB', **db_row.to_dict()})

        return pd.DataFrame(comparison)

    def save_comparison(self, df: pd.DataFrame, output_path: str):
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)

                workbook = writer.book
                worksheet = writer.sheets['Sheet1']

                for row in range(2, len(df) + 2):
                    if df.iloc[row - 2]['Source'] == 'DBF' and row < len(df) + 1:
                        if df.iloc[row - 1]['Source'] == 'DB':
                            for col in range(1, len(df.columns) + 1):
                                val1 = worksheet.cell(row=row, column=col).value
                                val2 = worksheet.cell(row=row + 1, column=col).value
                                if val1 != val2:
                                    worksheet.cell(row=row, column=col).fill = self.highlight_fill
                                    worksheet.cell(row=row + 1, column=col).fill = self.highlight_fill
        except Exception as e:
            self.logger.error(f"Ошибка сохранения сравнения: {str(e)}")
            raise