import pymysql
from Common.CEnum import AUTYPE, KL_TYPE, DATA_FIELD
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
            
            print(f"Executing SQL: {sql} with params: {params}")  # 添加调试信息
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            print(f"Found {len(rows)} rows of data")  # 添加调试信息
            
            for row in rows:
                # 解析日期并创建CTime对象
                date_str = str(row[0])
                year = int(date_str[:4])
                month = int(date_str[5:7])
                day = int(date_str[8:10])
                
                kline = CKLine_Unit({
                    DATA_FIELD.FIELD_TIME: CTime(year, month, day, 0, 0, auto=False),  # 添加auto=False
                    DATA_FIELD.FIELD_OPEN: float(row[1]),
                    DATA_FIELD.FIELD_HIGH: float(row[2]),
                    DATA_FIELD.FIELD_LOW: float(row[3]),
                    DATA_FIELD.FIELD_CLOSE: float(row[4]),
                    DATA_FIELD.FIELD_VOLUME: float(row[5]),
                    DATA_FIELD.FIELD_TURNOVER: float(row[6]),
                    DATA_FIELD.FIELD_TURNRATE: float(row[7] or 0)
                })
                yield kline
        finally:
            cursor.close()
            conn.close()
