import baostock as bs
import pymysql
from datetime import datetime
from Common.CEnum import KL_TYPE, AUTYPE

# 配置参数
CONFIG = {
    'begin_date': '2018-01-01',  
    'end_date': datetime.now().strftime('%Y-%m-%d'),
    'frequency': 'd',  # 周线数据
    'adjustflag': '2'  # 前复权
}

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

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def get_table_name(frequency):
    """根据频率返回对应的表名"""
    if frequency == 'w':
        return 'stock_history_w'
    elif frequency == 'd':
        return 'stock_history_d'
    elif frequency == 'm':
        return 'stock_history_m'
    elif frequency in ['5', '15', '30', '60']:
        return f'stock_history_{frequency}m'
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")

def create_table_if_not_exists(connection, table_name):
    cursor = connection.cursor()
    try:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            code VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            open DECIMAL(10,2) NOT NULL,
            high DECIMAL(10,2) NOT NULL,
            low DECIMAL(10,2) NOT NULL,
            close DECIMAL(10,2) NOT NULL,
            volume BIGINT NOT NULL,
            amount DECIMAL(20,2) NOT NULL,
            turn_rate DECIMAL(10,2),
            PRIMARY KEY (code, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
    finally:
        cursor.close()

def save_stock_data(connection, data_list, table_name):
    cursor = connection.cursor()
    try:
        sql = f"""INSERT INTO {table_name}
                (code, trade_date, open, high, low, close, volume, amount, turn_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume),
                amount=VALUES(amount), turn_rate=VALUES(turn_rate)"""
        
        cursor.executemany(sql, data_list)
        connection.commit()
        
    except Exception as e:
        print(f"Error saving data: {e}")
        connection.rollback()
    finally:
        cursor.close()

def get_stock_list():
    connection = get_db_connection()
    if connection is None:
        return []
    
    try:
        cursor = connection.cursor()
        # 从stock_list表中获取股票代码
        cursor.execute("SELECT code FROM stock_list")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching stock list from database: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_k_fields(frequency):
    """根据周期返回对应的字段列表"""
    if frequency in ['w', 'm']:
        # 周月线指标
        return "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
    else:
        # 日线及分钟线指标
        return "date,code,open,high,low,close,volume,amount,adjustflag"

def fetch_stock_data(code, begin_date=None, end_date=None):
    try:
        fields = get_k_fields(CONFIG['frequency'])
        rs = bs.query_history_k_data_plus(
            code=code,
            fields=fields,
            start_date=begin_date or CONFIG['begin_date'],
            end_date=end_date or CONFIG['end_date'],
            frequency=CONFIG['frequency'],
            adjustflag=CONFIG['adjustflag']
        )
        
        if rs.error_code != '0':
            print(f'Error getting data for {code}: {rs.error_msg}')
            return []
            
        data_list = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            # 根据频率处理不同的字段
            if CONFIG['frequency'] in ['w', 'm']:
                data_list.append((
                    row[1],                  # code
                    row[0],                  # date
                    float(row[2] or 0),      # open
                    float(row[3] or 0),      # high
                    float(row[4] or 0),      # low
                    float(row[5] or 0),      # close
                    int(float(row[6] or 0)), # volume
                    float(row[7] or 0),      # amount
                    float(row[9] or 0)       # turn
                ))
            else:
                data_list.append((
                    row[1],                  # code
                    row[0],                  # date
                    float(row[2] or 0),      # open
                    float(row[3] or 0),      # high
                    float(row[4] or 0),      # low
                    float(row[5] or 0),      # close
                    int(float(row[6] or 0)), # volume
                    float(row[7] or 0),      # amount
                    0.0                      # turn为0
                ))
        return data_list
    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        return []

def main():
    try:
        # 登录系统
        lg = bs.login()
        if lg.error_code != '0':
            print('login failed:', lg.error_msg)
            return
            
        # 获取股票列表
        stock_list = get_stock_list()
        
        # 连接数据库
        conn = get_db_connection()
        
        # 获取当前频率对应的表名
        table_name = get_table_name(CONFIG['frequency'])
        # 创建对应的表
        create_table_if_not_exists(conn, table_name)
        
        for code in stock_list:
            # 获取股票数据
            kline_data = fetch_stock_data(code, CONFIG['begin_date'], CONFIG['end_date'])
            
            if kline_data:
                # 传入对应的表名
                save_stock_data(conn, kline_data, table_name)
                print(f"Saved data for {code} in {table_name}")
            
        conn.close()
            
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        bs.logout()

if __name__ == "__main__":
    main()