import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
import baostock as bs

lg = bs.login()
print("Login response:", lg.error_msg)

rs = bs.query_history_k_data_plus(
    "sh.600519",
    "date,open,high,low,close,volume",
    start_date='2023-01-01',
    end_date='2023-12-31',
    frequency="d",
    adjustflag="2"  
)

data_list = []
while (rs.error_code == '0') & rs.next():
    data_list.append(rs.get_row_data())
if rs.error_code != '0':
    print("Data fetch error:", rs.error_msg)
  

df = pd.DataFrame(data_list, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
df['date'] = pd.to_datetime(df['date'])
for col in ['open', 'high', 'low', 'close', 'volume']:
    df[col] = df[col].astype(float)

bs.logout()
print("Data loaded. Shape:", df.shape)
df = df.sort_values('date').reset_index(drop=True)
print(df.head())


df['return'] = df['close'].pct_change()
daily_vol = df['return'].std()
annual_vol = daily_vol * np.sqrt(252)

df['cum_return'] = (1 + df['return']).cumprod() - 1

cummax = df['close'].cummax()
drawdown = (df['close'] - cummax) / cummax
max_drawdown = drawdown.min()

total_return = df['cum_return'].iloc[-1]
annual_return = (1 + total_return) ** (252 / len(df)) - 1
risk_free_rate = 0.02
sharpe = (annual_return - risk_free_rate) / annual_vol

print(f"Annual Return: {annual_return:.2%}")
print(f"Annual Volatility: {annual_vol:.2%}")
print(f"Max Drawdown: {max_drawdown:.2%}")
print(f"Sharpe Ratio: {sharpe:.2f}")

kline_df = df.set_index('date')[['open', 'high', 'low', 'close', 'volume']]
kline_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

mpf.plot(kline_df, type='candle', style='charles', volume=True,
         title='Kweichow Moutai 2023 Candlestick Chart',
         ylabel='Price (CNY)', ylabel_lower='Volume (lots)',
         savefig='kline.png')


df['MA5'] = df['close'].rolling(5).mean()
df['MA20'] = df['close'].rolling(20).mean()

golden_cross = (df['MA5'] > df['MA20']) & (df['MA5'].shift(1) <= df['MA20'].shift(1))
death_cross = (df['MA5'] < df['MA20']) & (df['MA5'].shift(1) >= df['MA20'].shift(1))

plt.figure(figsize=(14, 7))
plt.plot(df['date'], df['close'], linewidth=1, color='black', label='Close')
plt.plot(df['date'], df['MA5'], linestyle='--', linewidth=1, label='5-day MA')
plt.plot(df['date'], df['MA20'], linestyle='--', linewidth=1, label='20-day MA')

plt.scatter(df.loc[golden_cross, 'date'], df.loc[golden_cross, 'close'],
            marker='^', color='red', s=80, label='Golden Cross', zorder=5)
plt.scatter(df.loc[death_cross, 'date'], df.loc[death_cross, 'close'],
            marker='v', color='green', s=80, label='Death Cross', zorder=5)

plt.title('Kweichow Moutai 2023: Price with Moving Averages')
plt.xlabel('Date')
plt.ylabel('Price (CNY)')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.savefig('ma_signals.png')
plt.show()


plt.figure(figsize=(14, 6))
plt.plot(df['date'], df['cum_return'] * 100, color='green', linewidth=1.5, label='Cumulative Return (%)')
plt.axhline(y=0, color='red', linestyle='--', linewidth=1, label='Zero Line')
plt.title('2023 Cumulative Return Curve')
plt.xlabel('Date')
plt.ylabel('Cumulative Return (%)')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.savefig('cumulative_return.png')
plt.show()