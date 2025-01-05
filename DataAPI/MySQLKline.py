import pymysql
from Common.CEnum import AUTYPE, KL_TYPE
from Common.CTime import CTime
from DataAPI.CommonStockAPI import CCommonStockApi
from KLine.KLine_Unit import CKLine_Unit

class CMySQLKline(CCommonStockApi):
    def __init__(self, code, k_type=KL_TYPE.K_DAY, begin_date=None, end_date=None, autype=AUTYPE.QFQ):
        super(CMySQLKline, self).__init__(code, k_type, begin_date, end_date, autype)
        self.db_config = {
            'host': 'mysql.sqlpub.com',
            'port': 3306,
            'user': 'mysqlhq',
            'password': '7vHriuMuZw9HzAPl',
            'database': 'mysqlhq',
            'connect_timeout': 10,
            'read_timeout': 30,
            'write_timeout': 30
        }
        
    def get_table_name(self):
        ktype_map = {
            KL_TYPE.K_DAY: 'stock_history_d',
            KL_TYPE.K_WEEK: 'stock_history_w',
            KL_TYPE.K_MON: 'stock_history_m',
            KL_TYPE.K_30M: 'stock_history_30m'
        }
        return ktype_map.get(self.k_type)

    def get_kl_data(self):
        conn = pymysql.connect(**self.db_config)
        try:
            cursor = conn.cursor()
            table_name = self.get_table_name()
            where_clause = []
            params = []
            
            if self.begin_date:
                where_clause.append("trade_date >= %s")
                params.append(self.begin_date)
            if self.end_date:
                where_clause.append("trade_date <= %s")
                params.append(self.end_date)
            where_clause.append("code = %s")
            params.append(self.code)
            
            where_str = " AND ".join(where_clause)
            sql = f"""SELECT trade_date, open, high, low, close, volume, amount, turn_rate 
                     FROM {table_name} 
                     WHERE {where_str}
                     ORDER BY trade_date"""
            
            cursor.execute(sql, params)
            for row in cursor.fetchall():
                # 解析日期并创建CTime对象
                date_str = str(row[0])
                year = int(date_str[:4])
                month = int(date_str[5:7])
                day = int(date_str[8:10])
                
                yield CKLine_Unit({
                    'time': CTime(year, month, day, 0, 0),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5]),
                    'turnover': float(row[6]),
                    'turnrate': float(row[7] or 0)
                })
        finally:
            cursor.close()
            conn.close()
