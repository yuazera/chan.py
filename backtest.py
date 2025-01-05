from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, KL_TYPE
from strategies.chan_strategy import ChanStrategy

def run_backtest(code: str, start_date: str, end_date: str, initial_capital=100000, max_positions=3):
    """运行回测
    Args:
        code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        initial_capital: 初始资金
        max_positions: 最大持仓数量
    """
    config = CChanConfig({
        "trigger_step": True,
        "divergence_rate": 0.9,
        "min_zs_cnt": 1,
        "print_warning": True,  # 添加打印警告
    })
    
    chan = CChan(
        code=code,
        begin_time=start_date,
        end_time=end_date,
        data_src="custom:MySQLKline.CMySQLKline",
        lv_list=[KL_TYPE.K_DAY],
        config=config,
        autype=AUTYPE.QFQ
    )
    
    # 创建策略实例，设置初始资金和最大持仓数
    strategy = ChanStrategy(initial_capital=initial_capital, num=max_positions)
    
    # 逐K线计算
    k_count = 0
    for snapshot in chan.step_load():
        k_count += 1
        if len(snapshot[0]) < 2:  # 至少需要两根K线
            continue
            
        bsp_list = snapshot.get_bsp()
        if bsp_list:
            print(f"Found signal at {snapshot[0][-1][-1].time}: {bsp_list[-1].type}")
            
        if not bsp_list:
            continue
            
        cur_kline = snapshot[0][-1][-1]  # 当前K线
        last_bsp = bsp_list[-1]  # 最新买卖点
        
        # 打印更多信息用于调试
        print(f"Processing K-line at {cur_kline.time}, BSP type: {last_bsp.type}, is_buy: {last_bsp.is_buy}")
        
        # 确保信号在倒数第二根K线上
        if last_bsp.klu.klc.idx == snapshot[0][-2].idx:
            strategy.process_signal(
                code=code, 
                time=cur_kline.time.to_str(), 
                bsp=last_bsp,
                close_price=cur_kline.close
            )
    
    print(f"\nProcessed {k_count} K-lines")
    # 输出回测结果
    results = strategy.get_results()
    print(f"\n===== {code} 回测结果 =====")
    print(f"初始资金: {initial_capital:.2f}")
    print(f"最大持仓数: {max_positions}")
    print(f"总盈亏: {results['total_profit']:.2f}")
    print(f"胜率: {results['win_rate']:.2f}%")
    print(f"总交易次数: {results['total_trades']}")
    print(f"当前现金: {results['current_cash']:.2f}")
    print(f"当前持仓: {results['positions']}")
    return results

if __name__ == "__main__":
    code = "sh.601127"  
    start_date = "2023-01-01"
    end_date = "2024-12-31"
    run_backtest(
        code=code, 
        start_date=start_date, 
        end_date=end_date,
        initial_capital=100000,  # 10万初始资金
        max_positions=3  # 最多持有3只股票
    )
