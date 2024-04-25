import pandas as pd
import numpy as np
import threading
import time
from datetime import datetime
import os
from datetime import datetime
import warnings

start_time = time.time()

#Ignore FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

buy_target = 20
sell_target = 10
stop_loss = 30
days_after = 500

df = []
stocks_list = []
data_directory = input_excel =[]

data_directory = r'D:\Stocks\Strategy_Buy25_Sell10_Stop40'  # Replace with your directory path
input_excel = 'D:\Stocks\SP500\SP_Pull_List.xlsx'  # Replace with your input Excel file name
df = pd.read_excel(input_excel, usecols=[0,6])
stocks_list = df['Symbol'].tolist()  # Use the actual header name for the first column
#stock_exchange = df['Exchange'].tolist()
#stocks_list = stocks_list[110:130]
#stock_exchange = stock_exchange[:10]


def calculate_rsi(data, period=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    data['RSI'] = rsi
    data['RSI_Change'] = data['RSI'].diff()
    data['RSI_Status'] = data.apply(lambda row: 'Up' if row['RSI_Change'] > 0 else ('Down' if row['RSI_Change'] < 0 else 'Stable'), axis=1)
    return data[['RSI', 'RSI_Status']]


def calculate_macd(data, short_period=12, long_period=26, signal_period=9):
    # Calculate the short period EMA
    short_ema = data['Close'].ewm(span=short_period, adjust=False).mean()
    # Calculate the long period EMA
    long_ema = data['Close'].ewm(span=long_period, adjust=False).mean()
    # Calculate the MACD line
    data['MACD'] = short_ema - long_ema
    # Calculate the signal line
    data['Signal_Line'] = data['MACD'].ewm(span=signal_period, adjust=False).mean()
    # Calculate the difference between MACD and Signal Line
    data['MACD_Diff'] = data['MACD'] - data['Signal_Line']
    data['MACD_Status'] = data.apply(lambda row: 'Up' if row['MACD'] > row['Signal_Line'] else 'Down', axis=1)
    return data[['MACD', 'Signal_Line', 'MACD_Diff', 'MACD_Status']]


def calculate_ma(data):
    # Calculate Moving Averages
    data['MA_5'] = data['Close'].rolling(window=5).mean()
    data['MA_10'] = data['Close'].rolling(window=10).mean()
    data['MA_20'] = data['Close'].rolling(window=20).mean()

    # Compare Moving Averages and determine status
    data['MA_5_vs_10_Status'] = data.apply(lambda row: 'Up' if row['MA_5'] > row['MA_10'] else ('Down' if row['MA_5'] < row['MA_10'] else 'Stable'), axis=1)
    data['MA_10_vs_20_Status'] = data.apply(lambda row: 'Up' if row['MA_10'] > row['MA_20'] else ('Down' if row['MA_10'] < row['MA_20'] else 'Stable'),axis=1)

    # Compare Current Close vs Previous Close
    data['Close_Change'] = data['Close'].diff()
    data['Close_Status'] = data.apply(lambda row: 'Up' if row['Close_Change'] > 0 else ('Down' if row['Close_Change'] < 0 else 'Stable'), axis=1)
    return data[['MA_5', 'MA_10', 'MA_20', 'MA_5_vs_10_Status', 'MA_10_vs_20_Status', 'Close_Change', 'Close_Status']]

def calculate_vwap(data):
    # Calculate the typical price for each period, Volume-Weighted Average Price (VWAP)
    data['Typical_Price'] = (data['High'] + data['Low'] + data['Close']) / 3
    # Calculate the product of the typical price and the volume
    data['TPV'] = data['Typical_Price'] * data['Volume']
    # Calculate the cumulative total of TPV and the cumulative total of volume
    data['Cumulative_TPV'] = data['TPV'].cumsum()
    data['Cumulative_Volume'] = data['Volume'].cumsum()
    # Calculate VWAP
    data['VWAP'] = data['Cumulative_TPV'] / data['Cumulative_Volume']
    # Determine the trend status based on the Close price and VWAP
    data['VWAP_Status'] = data.apply(lambda row: 'Up' if row['Close'] > row['VWAP'] else 'Down', axis=1)
    return data[['Typical_Price', 'Volume', 'VWAP', 'VWAP_Status']]


def calculate_volume(data):
    # Calculate the change in volume from the previous period
    data['Volume_Change'] = data['Volume'].diff()

    # Determine the volume status based on the change
    data['Volume_Status'] = data.apply(lambda row: 'Up' if row['Volume_Change'] > 0 else ('Down' if row['Volume_Change'] < 0 else 'Stable'), axis=1)
    return data[['Volume_Change', 'Volume_Status']]


def calculate_obv(data):
    # Initialize OBV column
    data['OBV'] = 0

    # Ensure data is processed in order, avoiding SettingWithCopyWarning
    for i in range(1, len(data)):
        if data.loc[i, 'Close'] > data.loc[i - 1, 'Close']:
            data.loc[i, 'OBV'] = data.loc[i - 1, 'OBV'] + data.loc[i, 'Volume']
        elif data.loc[i, 'Close'] < data.loc[i - 1, 'Close']:
            data.loc[i, 'OBV'] = data.loc[i - 1, 'OBV'] - data.loc[i, 'Volume']
        else:
            data.loc[i, 'OBV'] = data.loc[i - 1, 'OBV']

    # Calculate OBV change and determine trend
    data['OBV_Change'] = data['OBV'].diff()
    data['OBV_Status'] = data.apply(lambda row: 'Up' if row['OBV_Change'] > 0 else ('Down' if row['OBV_Change'] < 0 else 'Stable'), axis=1)
    return data[['OBV', 'OBV_Change', 'OBV_Status']]


def calculate_score(row):
    # List of columns to check for "Up" status
    status_columns = ['RSI_Status', 'MACD_Status', 'Close_Status', 'MA_5_vs_10_Status',
                      'MA_10_vs_20_Status', 'Close_Status', 'VWAP_Status', 'Volume_Status', 'OBV_Status']
    # Count how many times "Up" occurs in the status columns for the row
    score = sum(row[col] == 'Up' for col in status_columns)
    return score


for count, stock in enumerate(stocks_list, start=1):
    try:
            holdings = ''
            time.sleep(1)
            Buy_Date_Temp = pd.to_datetime(datetime.today())
            print(f"Processing {count}/{len(stocks_list)}: {stock}")
            file_path = os.path.join(data_directory, f'{stock}.xlsx')
            df = pd.read_excel(file_path)
            # Calculate Historical High (maximum 'High' price in the period)
            historical_high = df['High'].max()
            df['HistoricalHigh'] = historical_high
            df['RunningHistoricalHigh'] = df['High'].cummax()

            if not df.empty and df['High'].notna().any():
                historical_high_date = df.loc[df['High'].idxmax(), 'Date']
            else:
                print("DataFrame is empty or 'High' column has no non-NaN data.")
                historical_high_date = None  # or an appropriate default/fallback value

            # Add a column for the date of the historical high
            df['DateOfHistoricalHigh'] = historical_high_date
            df[['RSI','RSI_Status']] = calculate_rsi(df, period=14)
            df[['MACD', 'Signal_Line', 'MACD_Diff', 'MACD_Status']] = calculate_macd(df)
            df[['MA_5', 'MA_10', 'MA_20', 'MA_5_vs_10_Status', 'MA_10_vs_20_Status', 'Close_Change', 'Close_Status']] = calculate_ma(df)
            df[['Typical_Price', 'Volume', 'VWAP', 'VWAP_Status']] = calculate_vwap(df)
            df[['Volume_Change', 'Volume_Status']] = calculate_volume(df)
            df[['OBV', 'OBV_Change', 'OBV_Status']] = calculate_obv(df)
            df['Score'] = df.apply(calculate_score, axis=1)
            df['score7'] = df['Score'].rolling(window=7, min_periods=1).mean().astype(int)
            df['score7'] = df['Score'].rolling(window=3, min_periods=1).mean().astype(int)


            # Calculate the difference in days between today's date and the historical high date
            today = pd.to_datetime("today")
            df['DaysSinceHistoricalHigh'] = (today - df['DateOfHistoricalHigh']).dt.days
            # Assuming df is your DataFrame after setting 'Date' to datetime and sorting
            df['Year'] = pd.NA
            df['Year'] = df['Date'].dt.year
            df['Date'] = pd.to_datetime(df['Date']).dt.date  # Convert dates to short format
            df.sort_values('Date', inplace=True)


            # Initialize placeholders
            df['RunningHistoricalHigh'] = pd.NA
            df['DateOfRunningHistoricalHigh'] = pd.NaT
            df['% Difference'] = pd.NA
            df['DateDifferenceFromPreviousHigh'] = pd.NA



            # Define start date for calculation to exclude the first year
            start_date_for_display = df['Date'].min() + pd.DateOffset(years=1)

            # Initialize variables to track the running historical high and its date
            running_historical_high = df[df['Date'] < start_date_for_display]['High'].max()
            date_of_running_historical_high = df[df['High'] == running_historical_high]['Date'].iloc[0]

            # Iterate through each row to update running historical highs
            for i, row in df.iterrows():
                if row['Date'] >= start_date_for_display:  # Start calculations from the second year
                    if row['High'] > running_historical_high:
                        running_historical_high = row['High']
                        date_of_running_historical_high = row['Date']

                    df.at[i, 'RunningHistoricalHigh'] = running_historical_high
                    df.at[i, 'DateOfRunningHistoricalHigh'] = date_of_running_historical_high

            # Forward fill to ensure all rows after the first year are filled
            df['RunningHistoricalHigh'].ffill(inplace=True)
            df['DateOfRunningHistoricalHigh'] = pd.to_datetime(df['DateOfRunningHistoricalHigh']).dt.date

            # Calculate "% Difference" in percentage format
            df['% Difference'] = ((df['RunningHistoricalHigh'] / df['Close']) - 1) * 100
            df['% Difference'] = df['% Difference'].apply(lambda x: f"{x:.2f}%")

            # Calculate "DateDifferenceFromPreviousHigh" ensuring to handle NaT properly
            df['DateOfRunningHistoricalHigh'] = pd.to_datetime(df['DateOfRunningHistoricalHigh'])
            df['DateDifferenceFromPreviousHigh'] = df['DateOfRunningHistoricalHigh'].diff().dt.days.fillna(0)
            df.loc[df['Date'] <= start_date_for_display, 'DateDifferenceFromPreviousHigh'] = pd.NA  #Reset for the first year

            # Convert 'Date' and 'DateRunHistHigh' to datetime for calculation
            df['Date'] = pd.to_datetime(df['Date'])
            df['DateOfRunningHistoricalHigh'] = pd.to_datetime(df['DateOfRunningHistoricalHigh'])

            # Calculate 'counter' as the difference in days between 'Date' and 'DateRunHistHigh'
            df['counter'] = (df['Date'] - df['DateOfRunningHistoricalHigh']).dt.days.fillna(0)

            # First, ensure that 'PctDiff' is using Pandas' nullable float type if it contains '<NA>'
            df['% Difference'] = pd.to_numeric(df['% Difference'].str.replace('%', ''), errors='coerce')

            # Initialize new columns
            # Assuming necessary columns are already created and 'Date' is in datetime format
            # Initialize 'signal' and 'Buy Date' for all rows
            df['signal'] = ''
            df['Buy Date'] = pd.NaT
            df['buy_period'] = 0
            df['Max30'] = np.nan
            df['Min30'] = np.nan

            # Prepare '% Difference' and 'flag' for condition checks
            # If '% Difference' might be a string with '%', try converting; otherwise, proceed
            if df['% Difference'].dtype == 'object':
                df['% Difference'] = pd.to_numeric(df['% Difference'].str.replace('%', ''), errors='coerce')
            else:
                # If it's already numeric or doesn't need conversion, you can skip or handle differently
                pass

            # Initialize or update 'flag' based on the conditions involving 'counter' and '% Difference'
            df['flag'] = ((df['counter'] > 365) & (df['% Difference'] < buy_target)).astype(int)

            # Assuming df is prepared with necessary columns and 'Date' is in datetime format

            # Track the last "buy" state
            last_buy_index = None
            df['Buy Date Temp'] = pd.NaT
            df['ltd'] = pd.NA
            df['ltd1'] = pd.NA
            ltd1_int = 500
            # df['Buy Date Temp'] = df['Buy Date'].max()
            df['Date'] = pd.to_datetime(df['Date'])
            df['Buy Date Temp'] = pd.to_datetime(df['Buy Date Temp'])


            #df['PreviousValidState'] = df['State'].fillna(method='ffill').shift(1)

            # Iterate through DataFrame to apply updated logic
            for i, row in df.iterrows():
                # Calculate 'buy_period' based on conditions
                if last_buy_index is not None:
                    df.at[i, 'buy_period'] = i - last_buy_index
                else:
                    df.at[i, 'buy_period'] = 0

                # Utilize the previous 'buy_period' value directly for logic conditions
                prev_buy_period = df.at[i - 1, 'buy_period'] if i > 1 else 0
                prev_signal = df.at[i - 1, 'signal'] if i > 1 else 0


                #df.at[i, 'ltd'] = (df.at[i, 'Date'] - Buy_Date_Temp).days

                # if i == df.index[0]:  # Check if the current index is the first in the DataFrame
                #     df.at[i, 'ltd'] = 0
                # else:
                #     df.at[i, 'ltd'] = (df.at[i, 'Date'] - Buy_Date_Temp).days

                # Find the last occurrence where 'signal' was 'Buy'
                last_buy_signal_index = df[df['signal'] == 'Buy'].last_valid_index()
                if last_buy_signal_index is not None:
                    # Retrieve the 'Buy Date' from the row where the last 'Buy' signal occurred
                    last_buy_date = df.at[last_buy_signal_index, 'Buy Date']
                    df.at[i, 'Buy Date Temp'] = df.at[last_buy_signal_index, 'Buy Date']
                    ltd = df.at[i, 'Date'] - df.at[i, 'Buy Date Temp']
                    #ltd1 = df.at[i, 'Date'] - df.at[i-1, 'Buy Date Temp']


                    if i == 0 or pd.isna(df.at[i - 1, 'Buy Date Temp']):
                        ltd1 = 0  # Set ltd1 to 0 if it's the first row or 'Buy Date Temp' is NA
                    else:
                        # Calculate the difference between 'Date' and 'Buy Date Temp' from the previous row
                        ltd1 = df.at[i, 'Date'] - df.at[i - 1, 'Buy Date Temp']

                    #check whether ltd1 is a Timedelta to safely extract days,
                    if pd.notnull(ltd1):
                        # Check if ltd1 is a Timedelta object
                        if isinstance(ltd1, pd.Timedelta):
                            ltd1_int = ltd1.days  # Extract the number of days as integer if it's Timedelta
                        elif isinstance(ltd1, int):
                            ltd1_int = ltd1  # Use directly if it's already an integer
                        else:
                            # Add here handling for other possible types if necessary, or convert to int
                            ltd1_int = int(ltd1)  # Convert to int, assuming it's convertible (like from float)
                    else:
                        ltd1_int = 0

                    df.at[i, 'ltd'] = ltd
                    df.at[i, 'ltd1'] = ltd1
                    #print(f"The last 'Buy' signal occurred on: {last_buy_date}")
                else:
                    df.at[i, 'ltd'] = 0
                    #print("There are no 'Buy' signals in the dataset.")

                # Apply conditions for setting signals
                if row['% Difference'] < sell_target and prev_buy_period > 0 and holdings == 'Buy':
                    df.at[i, 'signal'] = 'Success'
                    holdings = ''
                    df.at[i, 'Buy Date'] = row['Date']
                    df.at[i, 'buy_period'] = 0  # Increment previous buy_period

                #elif row['counter'] > 365 and row['% Difference'] < 8 and prev_buy_period == 0 and holdings == '' and not pd.isna(row['ltd1_int']) and row['ltd1_int'].days > 365:
                elif row['counter'] > days_after and row['% Difference'] < buy_target and prev_buy_period == 0 and holdings == '' and not pd.isna(ltd1_int) and ltd1_int > 365:

                    if prev_buy_period == 0:  # This effectively checks if the previous signal was not 'buy'
                        df.at[i, 'signal'] = 'Buy'
                        df.at[i, 'Buy Date'] = row['Date']
                        df.at[i, 'buy_period'] = 1  # Reset buy_period since this is a new buy signal
                        last_buy_index = i
                        end_date = row['Date'] + pd.Timedelta(days=30)
                        future_window = df[(df['Date'] > row['Date']) & (df['Date'] <= end_date)]
                        max_price = future_window['High'].max()
                        df.at[i, 'Max30'] = ((max_price - row['High'])/row['High']) * 100
                        future_window = df[(df['Date'] > row['Date']) & (df['Date'] <= end_date)]
                        min_price = future_window['Low'].min()
                        df.at[i, 'Min30'] = ((min_price - row['High']) / row['High']) * 100
                        holdings = 'Buy'
                        Buy_Date_Temp = row['Date']

                    else:
                        # If 'buy_period' > 0, it implies a continuation of a buy state; decide how to handle this case.
                        df.at[i, 'buy_period'] = prev_buy_period + 1  # Optionally increment buy_period if needed

                elif row['counter'] > days_after and row['% Difference'] < stop_loss and prev_buy_period > 0 and holdings == 'Buy':
                        # This effectively checks if the previous signal was not 'buy'
                        df.at[i, 'signal'] = ''
                        df.at[i, 'Buy Date'] = ''
                        holdings = 'Buy'
                        df.at[i, 'buy_period'] = prev_buy_period + 1

                elif row['counter'] > days_after and row['% Difference'] < stop_loss and prev_buy_period > 0:
                        # This effectively checks if the previous signal was not 'buy'
                        df.at[i, 'signal'] = ''
                        df.at[i, 'Buy Date'] = ''
                        holdings = ''
                        #df.at[i, 'buy_period'] = prev_buy_period + 1
                        df.at[i, 'buy_period'] = 0

                elif row['counter'] > days_after and row['% Difference'] > stop_loss and prev_buy_period > 0 and holdings == 'Buy':
                        df.at[i, 'signal'] = 'Fail'
                        df.at[i, 'Buy Date'] = row['Date']
                        df.at[i, 'buy_period'] = 0 # Increment previous buy_period
                        holdings = ''

                else:
                    df.at[i, 'signal'] = ''
                    df.at[i, 'buy_period'] = 0
                    df.at[i, 'Buy Date'] = ''
                    holdings = ''

            df['filled_signal'] = df['signal'].ffill()

            # Shift the 'filled_signal' column upwards to create the 'NextState' column
            # This makes each 'NextState' value represent the next non-null state from 'signal'
            df['NextState'] = df['filled_signal'].shift(-1)
            df['tt'] = df['buy_period'].shift(1)

            # Drop the intermediate 'filled_signal' column if you don't need it
            #df.drop(columns=['filled_signal'], inplace=True)

            # Convert 'Date', 'DateOfHistoricalHigh', and 'DateOfRunningHistoricalHigh' to date format without time
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df['DateOfHistoricalHigh'] = pd.to_datetime(df['DateOfHistoricalHigh']).dt.date
            df['DateOfRunningHistoricalHigh'] = pd.to_datetime(df['DateOfRunningHistoricalHigh']).dt.date

            df = df.rename(columns={
                'DateOfHistoricalHigh': 'HH_Date',
                'DateOfRunningHistoricalHigh': 'RHH_Date',
                'HistoricalHigh': 'HH',
                'RunningHistoricalHigh': 'RHH',
                'stock_name': 'Stock',
                'DaysSinceHistoricalHigh': 'DaysSinceHH'
                # Add more columns as needed
            })
            df.to_excel(file_path, index=False)
            print(f"DataFrame written to {file_path}")

    except Exception as e:
            #df.to_excel(file_path, index=False)
            print(f"Error occurred with {stock}{file_path}: {e}")
            continue
print("Job Done")
end_time = time.time()
# Calculate and print the total time
total_time = end_time - start_time
print(f"Total time taken: {total_time} seconds")