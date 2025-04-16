
""" Колонки, которые не будут сравниваться на соответствие (такие как NS, SN и т. д.) """
exclude_cols = ('SN', 'NS', 'RKEY', 'UID', 'TAL_N', 'DATS', 'DATPS', 'eu.NS', 'eu.RKEY')

""" Запрос для поиска по нашей базе. В качестве замены предусмотрены {SPN} и {eu.DATO}
 на соответствующие значения """
main_query = """
                SELECT * FROM exportfilep ep
                JOIN exportfileu eu ON ep.SN = eu.SN 
                AND eu.NS = (SELECT MAX(ep.NS) FROM exportfilep ep WHERE ep.SPN = '{SPN}')
                WHERE ep.NS = (SELECT MAX(ep.NS) FROM exportfilep ep WHERE ep.SPN = '{SPN}') 
                AND ep.SPN = '{SPN}' AND eu.DATO = '{DATO}'
            """

