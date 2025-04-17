from encodings.aliases import aliases

import pandas as pd
import pymysql
from pymysql import cursors
import logging
import os
from typing import Dict, Optional, List, Tuple
from config import client_policy_query, exportfile_query


class PatientSearcher:
    def __init__(self):
        self.stop_search = False
        self.logger = logging.getLogger('PatientSearcher')
        self.search_query = client_policy_query
        self.exportfile_query = exportfile_query
        # Основные таблицы и их возможные альтернативные названия
        self.table_aliases = {
            'exportfilep': ['exportfilep'],
            'exportfileu': ['exportfileu'],
            'exportfileo': ['exportfileo'],
            'exportfilel': ['exportfilel']
        }
        self.table_aliases_2 = {
            'exportfilep': ['vasP'],
            'exportfileu': ['`123`'],
            'exportfileo': ['vasO'],
            'exportfilel': ['vasL']
        }
        self.sn = None

    def search_patient(self, db_params: Dict) -> Optional[Dict]:
        """Поиск пациента и его данных в таблицах с учетом возможных альтернативных названий"""
        if self.stop_search:
            return None

        try:
            connection = pymysql.connect(
                host=db_params['host'],
                user=db_params['user'],
                password=db_params['password'],
                database=db_params['database'],
                cursorclass=cursors.DictCursor,
                connect_timeout=10
            )

            # 1. Находим полис клиента
            policy_data = self._find_client_policy(connection, db_params)
            if not policy_data:
                self.logger.warning(f"Пациент не найден в {db_params['database']}")
                return None

            # 2. Ищем данные в таблицах, проверяя возможные варианты названий
            exportfile_results = {}

            if db_params['database'] == 's12pays202504041322':
                aliaseses = self.table_aliases_2
            else:
                aliaseses = self.table_aliases

            for base_table, aliases in aliaseses.items():
                table_data = None
                for table_name in aliases:
                    try:
                        table_data = self._search_exportfile(connection, policy_data['number'], table_name,
                                                             db_params['database'])
                        if table_data:
                            # Сохраняем с основным названием таблицы для единообразия
                            exportfile_results[base_table] = table_data
                            break
                    except pymysql.Error as e:
                        self.logger.debug(
                            f"Таблица {table_name} не найдена в {db_params['database']}, пробуем следующий вариант")

                if not table_data:
                    self.logger.warning(f"Не удалось найти данные для таблицы {base_table} в {db_params['database']}")

            return {
                'policy': policy_data,
                'exportfiles': exportfile_results,
                'source_db': db_params['database']
            }

        except pymysql.Error as e:
            self.logger.error(f"Ошибка при поиске в {db_params['database']}: {str(e)}")
            raise
        finally:
            if 'connection' in locals() and connection:
                connection.close()

    def _find_client_policy(self, connection, db_params: Dict) -> Optional[Dict]:
        """Поиск полиса клиента (без изменений)"""
        with connection.cursor() as cursor:
            conditions = 'WHERE Client.deleted = 0 AND cp.deleted = 0'
            if db_params['patient_lastname']:
                conditions += f' AND Client.lastName = "{db_params["patient_lastname"]}"'
            if db_params['patient_firstname']:
                conditions += f' AND Client.firstName = "{db_params["patient_firstname"]}"'
            if db_params['patient_patrname']:
                conditions += f' AND Client.patrName = "{db_params["patient_patrname"]}"'

            query = self.search_query + '\n' + conditions + '\n LIMIT 1'
            print(f"Выполняем запрос: {query}")
            cursor.execute(query)
            return cursor.fetchone()

    def _search_exportfile(self, connection, client_id: int, table_name: str, db_name: str) -> List[Dict]:
        """Поиск данных в таблице с обработкой разных названий"""

        with connection.cursor() as cursor:
            try:
                if table_name.lower().startswith('exportfilep') or table_name.lower().startswith('vasp'):
                    query = self.exportfile_query.replace('exportfile', table_name).replace('{col}', 'tbl.SPN').replace(
                        '{value}', client_id)
                    if db_name == 's12':
                        query += f' AND tbl.NS = (SELECT MAX(NS) FROM {table_name} WHERE SPN = "{client_id}")'
                    print(f"Поиск в {table_name} (база {db_name}) для client_policy={client_id}: \n{query}")
                    cursor.execute(query)
                    result = cursor.fetchall()
                    if result:
                        self.sn = str(result[0]['SN'])
                    return result
                else:
                    if not self.sn:
                        return []
                    query = self.exportfile_query.replace('exportfile', table_name).replace('{col}', 'tbl.SN').replace(
                        '{value}', self.sn)
                    if db_name == 's12':
                        query += f' AND tbl.NS = (SELECT MAX(NS) FROM {table_name} WHERE SN = "{self.sn}")'
                    print(f"Поиск в {table_name} (база {db_name}) для SN={self.sn}: \n {query}")
                    cursor.execute(query)
                    return cursor.fetchall()
            except pymysql.Error as e:
                self.logger.debug(f"Ошибка при доступе к таблице {table_name}: {str(e)}")
                raise

    def save_results(
            self,
            db1_results: Optional[Dict],
            db2_results: Optional[Dict],
            output_path: str
    ) -> bool:
        """Сохранение результатов с учетом разных названий таблиц"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Сохраняем информацию о полисе
                self._save_policy_info(writer, db1_results, db2_results)

                # Сохраняем данные для каждой основной таблицы
                for base_table in self.table_aliases.keys():
                    self._save_table_data(writer, base_table, db1_results, db2_results)

                return True

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении результатов: {str(e)}")
            raise

    def _save_policy_info(self, writer, db1_results: Optional[Dict], db2_results: Optional[Dict]):
        """Сохранение информации о полисе (без изменений)"""
        policy_data = []

        if db1_results and 'policy' in db1_results:
            db1_policy = db1_results['policy'].copy()
            db1_policy['Источник'] = db1_results['source_db']
            policy_data.append(db1_policy)

        if db2_results and 'policy' in db2_results:
            db2_policy = db2_results['policy'].copy()
            db2_policy['Источник'] = db2_results['source_db']
            policy_data.append(db2_policy)

        if policy_data:
            policy_df = pd.DataFrame(policy_data)
            policy_df.to_excel(writer, sheet_name='Policy Info', index=False)
            self._adjust_column_width(writer.sheets['Policy Info'])

    def _save_table_data(self, writer, base_table: str, db1_results: Optional[Dict], db2_results: Optional[Dict]):
        """Сохраняет данные таблицы, учитывая что во второй базе название могло отличаться"""
        result_df = pd.DataFrame()

        # Добавляем данные из первой базы (используем основное название таблицы)
        if db1_results and base_table in db1_results['exportfiles']:
            db1_data = pd.DataFrame(db1_results['exportfiles'][base_table])
            db1_data['Источник'] = db1_results['source_db']
            result_df = pd.concat([result_df, db1_data])

        # Добавляем разделитель, если есть данные из обеих баз
        if not result_df.empty and db2_results:
            # Проверяем все возможные варианты названий таблицы во второй базе
            for alias in self.table_aliases[base_table]:
                if alias in db2_results['exportfiles']:
                    separator = pd.DataFrame({col: ['---'] for col in result_df.columns})
                    result_df = pd.concat([result_df, separator])

                    db2_data = pd.DataFrame(db2_results['exportfiles'][alias])
                    db2_data['Источник'] = db2_results['source_db']
                    result_df = pd.concat([result_df, db2_data])
                    break

        # Сохраняем только если есть данные
        if not result_df.empty:
            sheet_name = base_table[:31]  # Ограничение Excel на длину имени листа
            result_df.to_excel(writer, sheet_name=sheet_name, index=False)
            self._adjust_column_width(writer.sheets[sheet_name])

    def _adjust_column_width(self, worksheet):
        """Автоматическая настройка ширины столбцов"""
        for column in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in column)
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

    def compare_results(self, db1_results: Dict, db2_results: Dict) -> Dict:
        """Сравнение результатов с учетом разных названий таблиц"""
        comparison_result = {
            'policy': {
                'db1': db1_results.get('policy'),
                'db2': db2_results.get('policy')
            },
            'exportfiles': {}
        }

        for base_table, aliases in self.table_aliases.items():
            db1_data = db1_results.get('exportfiles', {}).get(base_table, [])

            # Ищем данные во второй базе по всем возможным названиям таблицы
            db2_data = []
            for alias in aliases:
                if alias in db2_results.get('exportfiles', {}):
                    db2_data = db2_results['exportfiles'][alias]
                    break

            comparison_result['exportfiles'][base_table] = {
                'db1': db1_data,
                'db2': db2_data
            }

        return comparison_result