import psycopg2
from datetime import datetime
import requests
import logging

import sys
sys.path.append("/home/taj/workspace/STOCK_MARKET/nsedata/nselib")
from nselib import derivatives


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection details
DB_CONFIG = {
    'dbname': 'nifty_options_data',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Slack webhook URL
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

# Bypass flag
BYPASS_MARKET_HOURS_CHECK = False  # Set to True to bypass the market hours check
SEND_ALERTS = True


def get_max_p_change_for_today(conn, identifier):
    query = '''
    SELECT MAX(p_change) 
    FROM option_data
    WHERE identifier = %s
    AND DATE(created_at) = %s;
    '''
    today = datetime.now().date()  # Get the current date

    with conn.cursor() as cur:
        cur.execute(query, (identifier, today))
        result = cur.fetchone()

        # Return the max p_change or None if no records found
        max_p_change = result[0] if result and result[0] is not None else None
        return max_p_change
    
# Connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Connected to PostgreSQL database!")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

# Create a table if it doesn't exist
def create_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS option_data (
        id SERIAL PRIMARY KEY,
        index_name VARCHAR(50),
        strike_price INTEGER,
        expiry_date DATE,
        open_interest INTEGER,
        change_in_open_interest INTEGER,
        p_change FLOAT,
        total_traded_volume INTEGER,
        implied_volatility FLOAT,
        last_price FLOAT,
        total_buy_quantity INTEGER,
        total_sell_quantity INTEGER,
        bid_qty INTEGER,
        bid_price FLOAT,
        ask_qty INTEGER,
        ask_price FLOAT,
        underlying_value FLOAT,
        option_type VARCHAR(10),
        identifier VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    '''
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        logging.info("Table created or already exists!")

# fetching options data from the nse using nselib
def fetch_data_from_nse(symbol):
    payload = derivatives.get_nse_option_chain(symbol).json()
    return payload

# Insert fetched data into the PostgreSQL database
def insert_data_into_db(conn, data):
    logging.info("Inserting data")
    
    insert_query = '''
    INSERT INTO option_data (
        index_name, strike_price, expiry_date, open_interest, change_in_open_interest,
        p_change, total_traded_volume, implied_volatility, last_price, total_buy_quantity,
        total_sell_quantity, bid_qty, bid_price, ask_qty, ask_price, underlying_value, option_type, identifier, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    alerts = []  # List to store alert messages
    threshold = 100  # Set your alert threshold percentage

    try:
        with conn.cursor() as cur:
            for option in data['filtered']['data']:
                underlying_value = option['PE']['underlyingValue'] if 'PE' in option else option['CE']['underlyingValue'] if 'CE' in option else None

                for option_type in ['PE', 'CE']:
                    if option_type in option and option[option_type]:
                        option_data = option[option_type]

                        # Check if the option is in the money
                        in_the_money = False
                        if option_type == 'PE':
                            # For PE (Put option), strike price must be greater than underlying value
                            if option['strikePrice'] > underlying_value:
                                in_the_money = True
                        elif option_type == 'CE':
                            # For CE (Call option), strike price must be less than underlying value
                            if option['strikePrice'] < underlying_value:
                                in_the_money = True

                        # Only process the first 10 ITM options for each type
                        if in_the_money:
                            max_p_change = get_max_p_change_for_today(conn, option_data["identifier"])

                            date_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

                            if max_p_change != None and abs(option_data['pChange']) >= threshold and abs(option_data['pChange']) >= max_p_change and SEND_ALERTS:
                                # Check for alerts based on p_change and in-the-money condition
                                alert_message = f"Alert: {date_time} {option_data['identifier']} option at strike price {option['strikePrice']} has p_change of {option_data['pChange']}% and is in the money (Underlying Value: {underlying_value})."
                                alerts.append(alert_message)
                                send_alert_to_slack(alert_message)  # Send alert to Slack

                            # Logging the current data being processed
                            logging.info(f"Inserting data for {option_data['underlying']} {option_type} at strike price {option['strikePrice']}.")

                            cur.execute(insert_query, (
                                option_data['underlying'],
                                option['strikePrice'],
                                option_data['expiryDate'],
                                option_data['openInterest'],
                                option_data['changeinOpenInterest'],
                                option_data['pChange'],
                                option_data['totalTradedVolume'],
                                option_data['impliedVolatility'],
                                option_data['lastPrice'],
                                option_data['totalBuyQuantity'],
                                option_data['totalSellQuantity'],
                                option_data['bidQty'],
                                option_data['bidprice'],
                                option_data['askQty'],
                                option_data['askPrice'],
                                option_data['underlyingValue'],
                                option_type,
                                option_data['identifier'],
                                datetime.now()
                            ))

            conn.commit()
            logging.info("Data inserted successfully!")
    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")

    return alerts  # Return alerts for processing

# Function to send alerts to Slack
def send_alert_to_slack(message):
    payload = {
        "text": message
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if response.status_code != 200:
        logging.error(f"Failed to send alert to Slack: {response.status_code}, {response.text}")

# Check if the current time is within market hours
def is_within_market_hours():
    current_time = datetime.now()
    return (current_time.weekday() < 5 and
            (current_time.hour == 9 and current_time.minute >= 16 or
             current_time.hour > 9 and current_time.hour < 15 or
             (current_time.hour == 15 and current_time.minute <= 30)))

# Main function
def main():
    if not BYPASS_MARKET_HOURS_CHECK and not is_within_market_hours():
        logging.warning("The script can only run between 9:16 AM and 3:30 PM on weekdays.")
        return

    conn = connect_to_db()
    if conn:
        create_table(conn)

        symbols = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'NIFTYNXT50']
        # symbols = ['NIFTY']
        
        # Fetch and insert data every minute until the end of market hours
        if BYPASS_MARKET_HOURS_CHECK or is_within_market_hours():
            for symbol in symbols:
                data = fetch_data_from_nse(symbol)
                if data:
                    alerts = insert_data_into_db(conn, data)
                    print(f"Alerts: {alerts}")
        else:
            print("Market is Closed!!")
            
    conn.close()

if __name__ == "__main__":
    main()
