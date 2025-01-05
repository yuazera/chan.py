from Common.CEnum import BSP_TYPE
from .base_strategy import BaseStrategy

class ChanStrategy(BaseStrategy):
    """缠论交易策略，处理一类买卖点信号"""
    
    def __init__(self, initial_capital=100000, num=3):  # 默认持仓3只股票
        """
        Args:
            initial_capital: 初始资金
            num: 最大持仓数量，资金会平均分配
        """
        super().__init__(initial_capital, num)

    def process_signal(self, code: str, time: str, bsp, close_price: float):
        """处理缠论买卖点信号"""
        print(f"Processing signal: time={time}, type={bsp.type}, is_buy={bsp.is_buy}")  # 添加调试信息
        
        # T1类买点（一买），且有资金和持仓空间
        if (BSP_TYPE.T1 in bsp.type or BSP_TYPE.T1P in bsp.type) and bsp.is_buy:
            print(f"Trying to buy at {time}")  # 添加调试信息
            result = self.buy(code, time, close_price)
            print(f"Buy result: {result}")  # 添加调试信息
            
        # T1类卖点（一卖），且持有该股票    
        elif (BSP_TYPE.T1 in bsp.type or BSP_TYPE.T1P in bsp.type) and not bsp.is_buy:
            print(f"Trying to sell at {time}")  # 添加调试信息
            result = self.sell(code, time, close_price)
            print(f"Sell result: {result}")  # 添加调试信息

# 必须添加下面这行，确保可以被正确导入
__all__ = ['ChanStrategy']
