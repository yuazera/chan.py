import pymysql
from cbsp import CBuyPointSignal
import argparse

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
    # 添加命令行参数解析
    parser = argparse.ArgumentParser(description='计算不同级别的买卖点信号')
    parser.add_argument('--levels', nargs='+', default=['w', 'd', '30'],
                       help='要处理的级别，可以是 w(周线)、d(日线)、30(30分钟) 中的一个或多个')
    args = parser.parse_args()

    # 验证级别参数
    valid_levels = {'w', 'd', '30'}
    input_levels = set(args.levels)
    if not input_levels.issubset(valid_levels):
        invalid_levels = input_levels - valid_levels
        print(f"错误：无效的级别参数 {invalid_levels}")
        print(f"有效的级别参数为: {valid_levels}")
        return

    try:
        for level in args.levels:
            if verify_and_create_tables(level):
                print(f"开始计算{level}级别买卖点...")
                cbsp = CBuyPointSignal(level=level)
                cbsp.calculate_signals()
                print(f"{level}级别买卖点计算完成")

    except Exception as e:
        print(f"计算买卖点时发生错误: {str(e)}")

if __name__ == "__main__":
    main()
