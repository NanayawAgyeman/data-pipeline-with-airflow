# Importing necessary libraries
import pandas as pd 
import yfinance as yf
from datetime import datetime
import os
from dotenv import load_dotenv

def run_yahoo_finance_etl(stock_symbols):
    """
    Function to perform ETL (Extract, Transform, Load) process on financial data from Yahoo Finance.

    The function extracts historical stock prices for specified symbols, refines the data,
    and stores it in separate CSV files in a 'stock_data' directory.

    Requirements:
    - yfinance library for fetching financial data.
    - Pandas for data manipulation.
    - dotenv for loading environment variables.

    Environment Variables:
    - None

    Inputs:
    - stock_symbols: List of stock symbols to fetch data for.

    Output:
    - CSV files stored in 'stock_data' directory, each named 'stock_prices_{symbol}.csv'
      containing refined financial data for each stock.

    Note: Ensure that the required environment variables are set before running the script.
    """
    # Create 'stock_data' directory if it doesn't exist
    if not os.path.exists('stock_data'):
        os.makedirs('stock_data')

    for symbol in stock_symbols:
        # Fetch historical stock prices using yfinance
        stock_data = yf.download(symbol, start="2022-01-01", end=datetime.today().strftime('%Y-%m-%d'))

        # Extract relevant information from the stock data and store in a DataFrame
        df = pd.DataFrame({
            'Date': stock_data.index,
            'Open': stock_data['Open'],
            'High': stock_data['High'],
            'Low': stock_data['Low'],
            'Close': stock_data['Close'],
            'Volume': stock_data['Volume']
        })

        # Save the DataFrame to a CSV file in the 'stock_data' directory for each stock
        csv_filename = f'stock_data/stock_prices_{symbol}.csv'
        df.to_csv(csv_filename, index=False)
        print(f"Data for {symbol} saved to {csv_filename}")

if __name__ == "__main__":
    # List of stock symbols to fetch data for
    stock_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NVDA', 'V', 'PYPL', 'NFLX']

    # Execute the ETL process for each stock
    run_yahoo_finance_etl(stock_symbols)
