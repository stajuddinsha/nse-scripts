import pandas as pd
from nselib import derivatives
from datetime import datetime

def format_expiry_date(expiry_date_str):
    # Convert expiry date from '31-Oct-2024' to '31-10-2024'
    return datetime.strptime(expiry_date_str, '%d-%b-%Y').strftime('%d-%m-%Y')

def fetch_and_print_recent_itm_options():
    # Get indexes with their expiry dates
    indexes_with_exp_dates = derivatives.expiry_dates_option_index()
    
    for index, expiry_dates in indexes_with_exp_dates.items():
        # Get the most recent expiry date
        most_recent_expiry_date = sorted(expiry_dates, key=lambda x: datetime.strptime(x, '%d-%b-%Y'))[-1]
        print(f"\nFetching live options data for {index} with expiry date: {most_recent_expiry_date}")

        try:
            # Fetch live option chain data for the index at the given formatted expiry date
            options_data = derivatives.nse_live_option_chain(index, format_expiry_date(most_recent_expiry_date))

            # Ensure the DataFrame is not empty
            if options_data.empty:
                print("No options data available.")
                continue

            print(options_data)
            
            # Filter for "In the Money" options (assuming we're looking at CALL options)
            # current_price = options_data['Strike_Price'].max()  # This should be the current underlying price
            # itm_options = options_data[options_data['Strike_Price'] < current_price]

            # # Calculate percent change and filter for >= 100%
            # itm_options['Percent_Change'] = ((itm_options['CALLS_LTP'] - itm_options['CALLS_LTP'].shift(1)) / itm_options['CALLS_LTP'].shift(1)) * 100

            # # Select options where Percent Change >= 100
            # alerts = itm_options[itm_options['Percent_Change'] >= 100].head(10)

            # if alerts.empty:
            #     print("No alerts for options with percentage change >= 100.")
            # else:
            #     print("Alerts for options with percentage change >= 100:")
            #     for _, row in alerts.iterrows():
            #         print({
            #             "Symbol": row['Symbol'],
            #             "Strike Price": row['Strike_Price'],
            #             "CALLS LTP": row['CALLS_LTP'],
            #             "Percent Change": row['Percent_Change'],
            #         })
        
        except Exception as e:
            print(f"Error fetching data for {index} on {most_recent_expiry_date}: {e}")

# Run the function to fetch and print recent ITM options
fetch_and_print_recent_itm_options()
