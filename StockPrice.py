import yfinance as yf
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

#load_dotenv("/home/kush0/projects/Flight-Price-Tracker/.env")
load_dotenv('./.env')
logging.basicConfig(filename=f'{os.getenv('LOG_PATH')}stock_price.log',
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

def get_stock_price(ticker):
    # Get stock information
    stock = yf.Ticker(ticker)
    stock_info = stock.history(period="1d")

    if stock_info.empty:
        logging.error(f"No data found for ticker '{ticker}'.")
        return None
    
    # Extract price
    price = stock_info['Close'].iloc[-1]
    price = round(price, 2) 
    logging.info(f"Successfully retrieved price for {ticker}: ${price:.2f}")

    return price

# Get prices
tickers = ['TSLA']
prices = {}
for ticker in tickers:
    price = get_stock_price('TSLA')
    prices[ticker] = price

# Create df
today = datetime.now().date()
data = []

for ticker, price in prices.items():
    if price is not None:
        data.append({
            'date': today,
            'ticker': ticker,
            'price': price
        })
    else:
        logging.warning(f'Price for {ticker} is None, skipping row creation.')
df = pd.DataFrame(data)

# Connect to DB
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_DATABASE')  

connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)

# Upload to database
try:
    df.to_sql('stock_prices', con=engine, if_exists='append', index=False)
    logging.info("Data uploaded to 'stock_prices' table successfully.")
except Exception as e:
    logging.error(f"Failed to upload to DB: {e}")

engine.dispose()