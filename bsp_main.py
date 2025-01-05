import pymysql
from cbsp import CBuyPointSignal

def verify_and_create_tables(level):
    """验证和创建特定级别的表"""
    cbsp = CBuyPointSignal(level=level)
    conn = cbsp.get_db_connection()
    try:
        # 创建买卖点表
        bsp_table = cbsp.get_table_name()
        with conn.cursor() as cursor:
            sql = f"""
            CREATE TABLE IF NOT EXISTS {bsp_table} (
                code VARCHAR(10) NOT NULL,
                direct VARCHAR(10) NOT NULL,
                type VARCHAR(10) NOT NULL,
                ts VARCHAR(30) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                note TEXT,
                PRIMARY KEY (code, ts)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(sql)
            
        # 验证行情表是否存在
        hq_table = cbsp.get_hq_table_name()
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = '{cbsp.db_config['database']}' 
                AND table_name = '{hq_table}'
            """)
            if cursor.fetchone()[0] == 0:
                print(f"Warning: Table {hq_table} does not exist")
                return False
        return True
    finally:
        conn.close()

def main():
    try:
        # 周线级别
        if verify_and_create_tables('w'):
            print("开始计算周线级别买卖点...")
            cbsp_week = CBuyPointSignal(level='w')
            cbsp_week.calculate_signals()
            print("周线级别买卖点计算完成")

        # 日线级别
        if verify_and_create_tables('d'):
            print("开始计算日线级别买卖点...")
            cbsp_day = CBuyPointSignal(level='d')
            cbsp_day.calculate_signals()
            print("日线级别买卖点计算完成")

        # 30分钟级别
        if verify_and_create_tables('30'):
            print("开始计算30分钟级别买卖点...")
            cbsp_30m = CBuyPointSignal(level='30')
            cbsp_30m.calculate_signals()
            print("30分钟级别买卖点计算完成")

    except Exception as e:
        print(f"计算买卖点时发生错误: {str(e)}")

if __name__ == "__main__":
    main()
