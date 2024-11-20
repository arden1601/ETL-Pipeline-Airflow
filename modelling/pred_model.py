import pandas as pd
import psycopg2 as ps
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller

try:
    conn = ps.connect(
        database="postgres",
        user="postgres.zdwwkvjomlsqnekrrmda",
        password="kc!e8ipt55h5jKH",
        host="aws-0-ap-southeast-1.pooler.supabase.com",
        port="6543"
    )
    print("Connected to database")  
except Exception as e:
    print("Error connecting database: ",e)

cur = conn.cursor()
cur.execute("SELECT * FROM public.pollution_data")

data = cur.fetchall()
columns = [desc[0] for desc in cur.description]

df = pd.DataFrame(data, columns=columns)
print(df.head())
cur.close()
conn.close()


df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# Ensure the data is sorted by date
df = df.sort_index()

# Check for missing values
df = df.ffill()

# df[['pm10', 'pm2_5']].plot(figsize=(12, 6), title="Pollution Levels")
# plt.show()


# result = adfuller(df['pm10'])
# print('ADF Statistic:', result[0])
# print('p-value:', result[1])

from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
df['hour'] = df.index.hour
df['pm10_lag1'] = df['pm10'].shift(1)
df.dropna(inplace=True)  # Drop rows with NaN after creating lags

# Define features and target
X = df[['hour', 'pm10_lag1']]
y = df['pm10']


# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the model
model = XGBRegressor()
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)
print("MSE:", mean_squared_error(y_test, y_pred))

# Predict for a specific hour tomorrow
specific_time_input = pd.DataFrame({
    'hour': [10],              # Example: 10 AM
    'pm10_lag1': [df['pm10'].iloc[-1]]  # Last observed PM10 value
})

predicted_value = model.predict(specific_time_input)
print("Predicted value for 10 AM tomorrow:", predicted_value[0])




