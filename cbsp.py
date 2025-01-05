import pymysql
from datetime import datetime, timedelta
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE

class CBuyPointSignal:
    def __init__(self, level='d'):
        # 数据库配置
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
        
        # Chan配置
        self.chan_config = CChanConfig({
            "bi_strict": True,
            "bs_type": '1,2,3a,1p,2s,3b',
            "print_warning": False,
        })
        
        # 定义支持的级别映射 - 修正月线的枚举值
        self.level_mapping = {
            'm': KL_TYPE.K_MON,    # 月线
            'w': KL_TYPE.K_WEEK,   # 周线
            'd': KL_TYPE.K_DAY,    # 日线
            '30': KL_TYPE.K_30M,   # 30分钟线
        }
        
        # 验证级别是否支持
        if level not in self.level_mapping:
            raise ValueError(f"Unsupported level: {level}. Supported levels are: {list(self.level_mapping.keys())}")
            
        self.level = level
        # 根据 level 设置对应的 KL_TYPE
        self.kl_type = self.level_mapping[level]
        
        # 应用配置
        self.app_config = {
            "delete_existing": False,
            "begin_date": "2018-10-01",
            "level": level
        }

    def get_db_connection(self):
        return pymysql.connect(**self.db_config)

    def get_stock_codes(self, conn):
        with conn.cursor() as cursor:
            cursor.execute("SELECT code FROM stock_list")
            return [row[0] for row in cursor.fetchall()]

    def get_table_name(self):
        """生成买卖点表名，每个级别一张表"""
        return f"bsp_{self.level}"
        
    def get_hq_table_name(self):
        """生成行情表名，每个级别一张表"""
        return f"stock_history_{self.level}"

    def save_bsp_to_db(self, conn, code, bsp_list):
        table_name = self.get_table_name()  # 不再需要传入 code
        with conn.cursor() as cursor:
            if self.app_config["delete_existing"]:
                cursor.execute(f"DELETE FROM {table_name} WHERE code = %s", (code,))
            
            sql = f"""INSERT INTO {table_name}
                    (code, direct, type, ts, price, note) 
                    VALUES (%s, %s, %s, %s, %s, %s)"""
                    
            for bsp in bsp_list:
                direction = "买入" if bsp.is_buy else "卖出"
                ts = f"{bsp.klu.time.to_str()}T00:00:00+08:00"
                price = bsp.klu.open
                bs_type = str(bsp.type).split("'")[1] if "'" in str(bsp.type) else str(bsp.type)
                note = f"{code},{bsp.klu.time.to_str()},{price},{bs_type}类{'买' if bsp.is_buy else '卖'}点"
                
                print(f"Inserting into {table_name}: {code} | {direction} | 类型{bs_type} | {ts} | 价格{price}")
                
                cursor.execute(sql, (code, direction, bs_type, ts, price, note))
        conn.commit()

    def calculate_signals(self):
        conn = self.get_db_connection()
        try:
            stock_codes = self.get_stock_codes(conn)
            
            # 验证行情表是否存在
            hq_table = self.get_hq_table_name()
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = '{self.db_config['database']}' 
                    AND table_name = '{hq_table}'
                """)
                if cursor.fetchone()[0] == 0:
                    raise ValueError(f"Table {hq_table} does not exist")
            
            for code in stock_codes:
                try:
                    print(f"Processing {code}...")
                    chan = CChan(
                        code=code,
                        begin_time=self.app_config["begin_date"],
                        end_time=None,
                        data_src=DATA_SRC.BAO_STOCK,
                        lv_list=[self.kl_type],
                        config=self.chan_config,
                        autype=AUTYPE.QFQ,
                    )
                    
                    bsp_list = chan.get_bsp()
                    self.save_bsp_to_db(conn, code, bsp_list)
                    print(f"Completed {code}: found {len(bsp_list)} points")
                    
                except Exception as e:
                    print(f"Error processing {code}: {e}")
                    continue
        finally:
            conn.close()
