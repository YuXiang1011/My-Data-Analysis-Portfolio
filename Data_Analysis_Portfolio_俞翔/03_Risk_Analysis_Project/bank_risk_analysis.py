import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 连接数据库（如果文件不存在会自动创建）
conn = sqlite3.connect('bank.db')
cursor = conn.cursor()

# 创建客户表
cursor.execute('''
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    age INTEGER,
    income REAL,
    credit_score INTEGER,
    city TEXT
)
''')

# 创建账户表
cursor.execute('''
CREATE TABLE IF NOT EXISTS accounts (
    account_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    account_type TEXT,
    balance REAL,
    open_date TEXT
)
''')

# 创建交易表
cursor.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    trans_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    trans_date TEXT,
    amount REAL,
    description TEXT
)
''')

# ------------------------------
# 1. 生成客户数据
# ------------------------------
np.random.seed(42)
n = 100
customer_ids = range(1, n+1)
names = [f'客户{i}' for i in customer_ids]
ages = np.random.randint(18, 70, n)
incomes = np.random.uniform(3000, 50000, n).round(2)   # 数组可以使用 .round()
credit_scores = np.random.randint(300, 850, n)
cities = np.random.choice(['北京', '上海', '广州', '深圳', '成都'], n)

customers_df = pd.DataFrame({
    'customer_id': customer_ids,
    'name': names,
    'age': ages,
    'income': incomes,
    'credit_score': credit_scores,
    'city': cities
})
customers_df.to_sql('customers', conn, if_exists='replace', index=False)
print("客户表已生成")

# ------------------------------
# 2. 生成账户数据
# ------------------------------
accounts_list = []
acc_id = 1
for cid in customer_ids:
    num_acc = np.random.choice([1, 2], p=[0.7, 0.3])
    for _ in range(num_acc):
        acc_type = np.random.choice(['储蓄', '理财', '信用卡'], p=[0.5, 0.3, 0.2])
        # 修正：使用 round() 函数，不是 .round() 方法
        balance = round(np.random.uniform(0, 200000), 2)
        open_date = pd.Timestamp('2020-01-01') + pd.Timedelta(days=np.random.randint(0, 1800))
        accounts_list.append([acc_id, cid, acc_type, balance, open_date.strftime('%Y-%m-%d')])
        acc_id += 1
accounts_df = pd.DataFrame(accounts_list, columns=['account_id', 'customer_id', 'account_type', 'balance', 'open_date'])
accounts_df.to_sql('accounts', conn, if_exists='replace', index=False)
print(f"账户表已生成，共 {len(accounts_df)} 个账户")

# ------------------------------
# 3. 生成交易数据
# ------------------------------
trans_list = []
trans_id = 1
start_date = pd.Timestamp('2023-01-01')
end_date = pd.Timestamp('2024-12-31')

for acc_id in accounts_df['account_id']:
    n_trans = np.random.randint(10, 51)
    trans_dates = pd.date_range(start_date, end_date, periods=n_trans)
    for i in range(n_trans):
        # 修正：使用 round() 函数
        amount = round(np.random.normal(500, 200), 2)
        if amount < 0:
            amount = 10
        if np.random.rand() < 0.05:
            amount = round(np.random.uniform(10000, 50000), 2)
        desc = np.random.choice(['消费', '转账', '取现', '理财申购', '还款'])
        trans_list.append([trans_id, acc_id, trans_dates[i].strftime('%Y-%m-%d'), amount, desc])
        trans_id += 1

transactions_df = pd.DataFrame(trans_list, columns=['trans_id', 'account_id', 'trans_date', 'amount', 'description'])
transactions_df.to_sql('transactions', conn, if_exists='replace', index=False)
print(f"交易表已生成，共 {len(transactions_df)} 笔交易")

conn.close()
print("数据库 bank.db 创建完成")







import sqlite3
import pandas as pd
import numpy as np
conn = sqlite3.connect('bank.db')
df_cust = pd.read_sql_query("SELECT * FROM customers", conn)
conn.close()


def normalize(s):
    return (s-s.min())/(s.max()-s.min())

df_cust["age_norm"]=normalize(df_cust["age"])
df_cust["income_norm"]=1-normalize(df_cust["income"])
df_cust["credit_norm"]=1-normalize(df_cust["credit_score"])

df_cust["risk_score"]=df_cust["age_norm"]*0.2+df_cust["income_norm"]*0.3+df_cust["credit_norm"]*0.5
df_cust["risk_level"]=pd.cut(df_cust["risk_score"],bins=[0,0.3,0.6,1],labels=["低风险","中风险","高风险"])

print(df_cust[['customer_id', 'name', 'risk_score', 'risk_level']].head())

df_cust.to_csv("customer_risk.csv",index=False)
print("客户风险评分已保存到 customer_risk.csv")


conn=sqlite3.connect("bank.db")

query='''
select t.trans_id,t.trans_date,t.description,a.customer_id,c.name,t.amount
from transactions as t
join accounts as a on t.account_id=a.account_id
join customers as c on c.customer_id=a.customer_id
'''

df_trans=pd.read_sql_query(query,conn)
conn.close()


stats=df_trans.groupby("customer_id")["amount"].agg(["mean","std"]).reset_index()
stats.columns=["customer_id","mean_amt","std_amt"]
stats["upper_limit"]=stats["mean_amt"]+3*stats["std_amt"]

df_trans=df_trans.merge(stats[["customer_id","upper_limit"]],on="customer_id")
df_trans['is_anomaly'] = df_trans['amount'] > df_trans['upper_limit']
anomalies = df_trans[df_trans['is_anomaly']].copy()
print(f"发现 {len(anomalies)} 笔异常交易")
print(anomalies[['trans_id', 'name', 'trans_date', 'amount', 'description']].head())

anomalies.to_csv('anomaly_transactions.csv', index=False)
print("异常交易已保存到 anomaly_transactions.csv")
