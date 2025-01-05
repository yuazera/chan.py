import pymysql
from datetime import datetime, timedelta
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE

# 数据库配置
DB_CONFIG = {
    'host': 'mysql.sqlpub.com',    # 只保留域名
    'port': 3306,              
    'user': 'mysqlhq',
    'password': '7vHriuMuZw9HzAPl',
    'database': 'mysqlhq',
    'connect_timeout': 10,         # 添加连接超时时间
    'read_timeout': 30,           # 添加读取超时时间
    'write_timeout': 30          # 添加写入超时时间
}

# Chan配置
CHAN_CONFIG = {
    "bi_strict": True,
    "bs_type": '1,2,3a,1p,2s,3b',
    "print_warning": False,
}

# 通用配置
APP_CONFIG = {
    "delete_existing": False,  # 是否删除已有记录 True/False
    "begin_date": "2018-10-01",    # 起始日期 yyyy-mm-dd
    "chan_config": CHAN_CONFIG,
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def get_stock_codes(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT code FROM stock_list")
        return [row[0] for row in cursor.fetchall()]

def save_bsp_to_db(conn, code, bsp_list, config):
    with conn.cursor() as cursor:
        # 根据配置决定是否删除已有记录
        if config.get("delete_existing", True):
            cursor.execute("DELETE FROM bsp_week WHERE code = %s", (code,))
        
        # 插入新的买卖点记录，适配新的表结构
        sql = """INSERT INTO bsp_week 
                (code, direct, type, ts, price, note) 
                VALUES (%s, %s, %s, %s, %s, %s)"""
                
        for bsp in bsp_list:
            # 转换买卖方向为中文
            direction = "买入" if bsp.is_buy else "卖出"
            # 生成时间戳格式
            ts = f"{bsp.klu.time.to_str()}T00:00:00+08:00"
            # 从K线单元中获取价格
            price = bsp.klu.open
            
            # 处理 bsp.type，提取引号中的内容
            bs_type = str(bsp.type).split("'")[1] if "'" in str(bsp.type) else str(bsp.type)
            
            # 生成备注信息
            note = f"{code},{bsp.klu.time.to_str()},{price},{bs_type}类{'买' if bsp.is_buy else '卖'}点"
            
            # 打印要插入的数据
            print(f"Inserting: {code} | {direction} | 类型{bs_type} | {ts} | 价格{price} | {note}")
            
            cursor.execute(sql, (
                code,
                direction,
                bs_type,  # 使用处理后的 type
                ts,
                price,
                note
            ))
    conn.commit()

def calculate_week_bsp():
    conn = get_db_connection()
    try:
        stock_codes = get_stock_codes(conn)
        
        # 直接使用默认配置
        chan_config = CChanConfig(CHAN_CONFIG)
        begin_time = APP_CONFIG["begin_date"]

        for code in stock_codes:
            try:
                print(f"Processing {code}...")
                
                chan = CChan(
                    code=code,
                    begin_time=begin_time,
                    end_time=None,
                    data_src=DATA_SRC.BAO_STOCK,
                    lv_list=[KL_TYPE.K_WEEK],
                    config=chan_config,
                    autype=AUTYPE.QFQ,
                )

                bsp_list = chan.get_bsp()
                save_bsp_to_db(conn, code, bsp_list, APP_CONFIG)
                
                print(f"Completed {code}: found {len(bsp_list)} points")
                
            except Exception as e:
                print(f"Error processing {code}: {str(e)}")
                continue

    finally:
        conn.close()

if __name__ == "__main__":
    calculate_week_bsp()
