import sys
sys.path.append("/home/taj/workspace/STOCK_MARKET/nsedata/nselib")
from nselib import derivatives

payload = derivatives.get_nse_option_chain("NIFTY").json()
print(payload)


