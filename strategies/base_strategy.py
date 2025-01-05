from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime

@dataclass
class Trade:
    time: str
    type: str 
    code: str
    price: float
    shares: int
    cost: float = 0
    revenue: float = 0
    profit: float = 0

class BaseStrategy:
    def __init__(self, initial_capital=100000, num=5):
        self.positions: Dict[str, dict] = {}  # 持仓记录
        self.trades: List[Trade] = []     # 修复这里的语法错误
        self.cash = initial_capital  # 初始资金
        self.max_positions = num     # 最大持仓数量
        self.position_value = initial_capital / num  # 每个持仓的资金额度
        
    def buy(self, code: str, time: str, price: float, shares: int = None):
        """买入操作"""
        if code in self.positions or len(self.positions) >= self.max_positions:
            return False
            
        if shares is None:  # 默认按照持仓数量平均分配资金
            shares = int(self.position_value / price)
            
        cost = shares * price
        if cost > self.cash:
            return False
            
        self.positions[code] = {'shares': shares, 'cost': cost}
        self.cash -= cost
        
        self.trades.append(Trade(
            time=time,
            type='BUY',
            code=code,
            price=price,
            shares=shares,
            cost=cost
        ))
        print(f"买入: {time} 代码:{code} 价格:{price} 数量:{shares}")
        return True
        
    def sell(self, code: str, time: str, price: float):
        """卖出操作"""
        if code not in self.positions:
            return False
            
        pos = self.positions[code]
        revenue = pos['shares'] * price
        profit = revenue - pos['cost']
        self.cash += revenue
        
        self.trades.append(Trade(
            time=time,
            type='SELL',
            code=code,
            price=price,
            shares=pos['shares'],
            revenue=revenue,
            profit=profit
        ))
        print(f"卖出: {time} 代码:{code} 价格:{price} 数量:{pos['shares']} 利润:{profit:.2f}")
        
        del self.positions[code]
        return True
        
    def get_results(self):
        """获取回测结果统计"""
        sell_trades = [t for t in self.trades if t.type == 'SELL']
        total_profit = sum([t.profit for t in sell_trades])
        win_trades = len([t for t in sell_trades if t.profit > 0])
        total_trades = len(sell_trades)
        
        return {
            'total_profit': total_profit,
            'win_rate': win_trades/total_trades*100 if total_trades > 0 else 0,
            'total_trades': total_trades,
            'current_cash': self.cash,
            'positions': self.positions
        }
