import baostock as bs
import pandas as pd
import numpy as np

# 登录
lg = bs.login()
print("Login response:", lg.error_msg)

# 定义要获取的指数代码和名称
indexes = {
    'CSI300': 'sh.000300',
    'Semiconductor': 'sz.399998',   # 中证半导体指数
    'Bond': 'sh.000012'             # 国债指数
}

data = {}
for name, code in indexes.items():
    print(f"Fetching {name} ({code})...")
    rs = bs.query_history_k_data_plus(
        code,
        "date,close",
        start_date='2025-01-01',
        end_date='2026-12-31',
        frequency="d",
        adjustflag="2"
    )
    rows = []
    while (rs.error_code == '0') & rs.next():
        rows.append(rs.get_row_data())
    if rs.error_code != '0':
        print(f"Error fetching {name}: {rs.error_msg}")
        continue
    df = pd.DataFrame(rows, columns=['date', 'close'])
    df['date'] = pd.to_datetime(df['date'])
    df['close'] = df['close'].astype(float)
    # 标准化为从1开始
    df['nav'] = df['close'] / df['close'].iloc[0]
    data[name] = df[['date', 'nav']].rename(columns={'nav': name})

bs.logout()

# 合并
df_merged = None
for name in data:
    if df_merged is None:
        df_merged = data[name]
    else:
        df_merged = df_merged.merge(data[name], on='date', how='inner')

print("数据合并完成，前5行：")
print(df_merged.head())
df_merged.to_csv('fund_data_index.csv', index=False)
import numpy as np
import pandas as pd

def calc_metrics(nav_series):
    """Calculate performance metrics for a single fund NAV series"""
    ret = nav_series.pct_change().dropna()
    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1
    annual_return = (1 + total_return) ** (252 / len(ret)) - 1
    annual_vol = ret.std() * np.sqrt(252)
    cummax = nav_series.cummax()
    drawdown = (nav_series - cummax) / cummax
    max_dd = drawdown.min()
    sharpe = (annual_return - 0.02) / annual_vol if annual_vol != 0 else np.nan
    # Return order: annual_return, annual_vol, max_dd, sharpe
    return annual_return, annual_vol, max_dd, sharpe

results = []
for fund in ["CSI300", "Semiconductor", "Bond"]:   # Use English column names
    ann_ret, ann_vol, mdd, sharpe = calc_metrics(df_merged[fund])
    results.append({
        'Fund': fund,
        'Annual Return': ann_ret,
        'Annual Volatility': ann_vol,
        'Max Drawdown': mdd,
        'Sharpe Ratio': sharpe
    })

metrics_df = pd.DataFrame(results)
metrics_df = metrics_df.sort_values('Sharpe Ratio', ascending=False)
print(metrics_df)
best_fund = metrics_df.iloc[0]['Fund']
print(f"\nBest fund (highest Sharpe Ratio): {best_fund}")

selected = df_merged[["date", best_fund]].copy()
selected.set_index("date", inplace=True)

# 使用 'ME' 代替 'M' 进行月末重采样
monthly_nav = selected.resample("ME").last()

monthly_invest = 1000
shares = monthly_invest / monthly_nav[best_fund]
total_shares = shares.sum()
total_invest = monthly_invest * len(monthly_nav)
final_value = total_shares * monthly_nav[best_fund].iloc[-1]
drip_return = (final_value / total_invest - 1) * 100

print(f"Total Drip Investment: {total_invest:.0f} CNY")
print(f"Total Shares: {total_shares:.2f}")
print(f"Final Market Value: {final_value:.2f} CNY")
print(f"Drip Return: {drip_return:.2f}%")

lump_sum_invest = total_invest
initial_nav = monthly_nav[best_fund].iloc[0]
shares_lump = lump_sum_invest / initial_nav
final_value_lump = shares_lump * monthly_nav[best_fund].iloc[-1]
lump_return = (final_value_lump / lump_sum_invest - 1) * 100
print(f"一次性投资收益率：{lump_return:.2f}%")
print(f"定投 vs 一次性：{drip_return - lump_return:.2f}%")