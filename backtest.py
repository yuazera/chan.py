from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, BSP_TYPE, KL_TYPE
from datetime import datetime
from decimal import Decimal

class Strategy:
    def __init__(self):
        self.positions = {}  # 持仓记录
        self.trades = []     # 交易记录
        self.cash = 100000   # 初始资金
        
    def process_signal(self, code, time, bsp, close_price):
        """处理买卖信号"""
        if BSP_TYPE.T1 in bsp.type and bsp.is_buy and code not in self.positions:
            # 一类买点，全仓买入
            shares = int(self.cash / close_price)
            cost = shares * close_price
            self.positions[code] = {'shares': shares, 'cost': cost}
            self.cash -= cost
            self.trades.append({
                'time': time,
                'type': 'BUY',
                'code': code,
                'price': close_price,
                'shares': shares,
                'cost': cost
            })
            print(f"买入: {time} 代码:{code} 价格:{close_price} 数量:{shares}")
            
        elif BSP_TYPE.T1 in bsp.type and not bsp.is_buy and code in self.positions:
            # 一类卖点，全部卖出
            pos = self.positions[code]
            revenue = pos['shares'] * close_price
            profit = revenue - pos['cost']
            self.cash += revenue
            self.trades.append({
                'time': time,
                'type': 'SELL',
                'code': code,
                'price': close_price,
                'shares': pos['shares'],
                'revenue': revenue,
                'profit': profit
            })
            print(f"卖出: {time} 代码:{code} 价格:{close_price} 数量:{pos['shares']} 利润:{profit:.2f}")
            del self.positions[code]

def run_backtest(code, start_date, end_date):
    """运行回测"""
    config = CChanConfig({
        "trigger_step": True,  # 启用逐步加载模式
        "divergence_rate": 0.9,
        "min_zs_cnt": 1
    })
    
    chan = CChan(
        code=code,
        begin_time=start_date,
        end_time=end_date,
        data_src="custom:MySQLKline.CMySQLKline",
        lv_list=[KL_TYPE.K_DAY],  # 使用日线级别
        config=config,
        autype=AUTYPE.QFQ
    )
    
    strategy = Strategy()
    
    # 逐K线计算
    for snapshot in chan.step_load():
        if len(snapshot[0]) < 2:  # 至少需要两根K线才能计算
            continue
            
        bsp_list = snapshot.get_bsp()
        if not bsp_list:
            continue
            
        # 获取最后一根K线的收盘价
        cur_kline = snapshot[0][-1][-1]
        close_price = cur_kline.close
        
        # 处理最新的买卖点信号
        last_bsp = bsp_list[-1]
        if last_bsp.klu.klc.idx == snapshot[0][-2].idx:  # 确保信号在倒数第二根K线上
            strategy.process_signal(code, cur_kline.time.to_str(), last_bsp, close_price)
    
    # 计算回测结果
    total_profit = sum([trade['profit'] for trade in strategy.trades if trade['type'] == 'SELL'])
    win_trades = len([trade for trade in strategy.trades if trade['type'] == 'SELL' and trade['profit'] > 0])
    total_trades = len([trade for trade in strategy.trades if trade['type'] == 'SELL'])
    
    print("\n===== 回测结果 =====")
    print(f"总盈亏: {total_profit:.2f}")
    print(f"胜率: {win_trades/total_trades*100:.2f}%" if total_trades > 0 else "无交易")
    print(f"总交易次数: {total_trades}")
    print(f"当前现金: {strategy.cash:.2f}")
    print(f"持仓: {strategy.positions}")

if __name__ == "__main__":
    code = "sh.600000"  # 浦发银行
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    run_backtest(code, start_date, end_date)
