from slack_sdk.webhook import WebhookClient
import time
import datetime
import sys
sys.path.append("/home/taj/workspace/STOCK_MARKET/nsedata/nselib")
from nselib import derivatives

# Slack webhook URL (replace with your actual URL)
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
webhook = WebhookClient(SLACK_WEBHOOK_URL)

# Flag to enable/disable sending alerts to Slack
SEND_TO_SLACK = False

# Function to send alerts (Slack or console)
def send_alert(message):
    if SEND_TO_SLACK:
        webhook.send(text=message)
    else:
        print(message)

# Function to fetch option chain data
def fetch_option_chain(symbol):
    try:
        payload = derivatives.get_nse_option_chain(symbol).json()
        return payload['filtered']['data']
    except Exception as e:
        send_alert(f"Error fetching option chain data: {e}")
        return []

# Function to calculate overall PCR
def calculate_pcr(option_chain):
    total_put_oi = sum(entry['PE']['openInterest'] for entry in option_chain if 'PE' in entry)
    total_call_oi = sum(entry['CE']['openInterest'] for entry in option_chain if 'CE' in entry)
    return total_put_oi / total_call_oi if total_call_oi != 0 else 0

# Function to identify directional moves
def analyze_directional_moves(option_chain):
    messages = []
    for entry in option_chain:
        strike_price = entry['strikePrice']

        # Call (CE) Analysis
        if 'CE' in entry:
            ce_data = entry['CE']
            if ce_data['changeinOpenInterest'] > 10000 and ce_data['impliedVolatility'] > 50:
                messages.append(f"📈 Bullish Move Likely: Strike {strike_price}")

        # Put (PE) Analysis
        if 'PE' in entry:
            pe_data = entry['PE']
            if pe_data['changeinOpenInterest'] > 10000 and pe_data['impliedVolatility'] > 50:
                messages.append(f"📉 Bearish Move Likely: Strike {strike_price}")
    return messages

# Function to analyze skew and max pain
def analyze_skew_and_max_pain(option_chain):
    max_pain_strike = None
    total_loss = float('inf')

    for entry in option_chain:
        strike_price = entry['strikePrice']
        call_oi = entry['CE']['openInterest'] if 'CE' in entry else 0
        put_oi = entry['PE']['openInterest'] if 'PE' in entry else 0

        # Max Pain Calculation
        call_loss = call_oi * abs(strike_price - entry['CE']['underlyingValue'])
        put_loss = put_oi * abs(strike_price - entry['PE']['underlyingValue'])
        total_strike_loss = call_loss + put_loss

        if total_strike_loss < total_loss:
            total_loss = total_strike_loss
            max_pain_strike = strike_price

    return max_pain_strike

# Main function to analyze and alert
def analyze_and_alert(symbol):
    option_chain = fetch_option_chain(symbol)
    if not option_chain:
        return

    # Calculate PCR
    pcr = calculate_pcr(option_chain)
    send_alert(f"Current PCR: {pcr:.2f}")

    # Analyze directional moves
    directional_messages = analyze_directional_moves(option_chain)
    for message in directional_messages:
        send_alert(message)

    # Analyze max pain
    max_pain_strike = analyze_skew_and_max_pain(option_chain)
    send_alert(f"Max Pain Strike: {max_pain_strike}")

# Function to check if the script should run
def should_run():
    now = datetime.datetime.now()
    # Check if today is a weekend
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    # Check if current time is within 9:15 AM to 3:30 PM
    start_time = datetime.time(9, 15)
    end_time = datetime.time(15, 30)
    return start_time <= now.time() <= end_time

# Continuous monitoring
if __name__ == "__main__":
    symbol = "NIFTY"
    print(f"Monitoring {symbol} option chain... Script will run between 9:15 AM and 3:30 PM on weekdays.")
    try:
        while True:
            if should_run():
                analyze_and_alert(symbol)
                time.sleep(5)  # Wait for 5 seconds
            else:
                print("Outside trading hours or weekend. Sleeping for 60 seconds.")
                time.sleep(60)  # Wait for 1 minute before checking again
    except KeyboardInterrupt:
        print("\nTerminated by user.")
