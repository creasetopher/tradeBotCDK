# THIS FILE SHOULD BE DEPRECATED.
# THE ACTUAL UTILS ARE IN utils/utils.py

import requests
import asyncio
import boto3
import yfinance as yf
import yfscreen as yfs
from openai_client import decision_model
from objects.types import ParsedQuote
import os




def realtime_data_callback(message: str):
    #     {
    # "id": "BTC-USD",
    # "price": 87690.22,
    # "time": 1735364458000,
    # "currency": "USD",
    # "exchange": "CCC",
    # "quoteType": "NONE",
    # "marketHours": "PRE_MARKET",
    # "changePercent": 0.1,
    # "dayVolume": 0,
    # "dayHigh": 0.1,
    # "dayLow": 0.1,
    # "change": 0.1,
    # "shortName": "string",
    # "openPrice": 0.1,
    # "previousClose": 0.1,
    # "bid": 0.1,
    # "bidSize": 0,
    # "ask": 0.1,
    # "askSize": 0,
    # "marketCap": 0.1
    # }
    # This function will be called every time a new message is received from the websocket
    # ultimately, this is where you would process the incoming data and potentially trigger your analysis/model

    update_message = json.loads(message)
    print(f"Update received: {message}\n\n")

async def async_get_realtime_data(symbols: list[str]):
    
    # 2. Initialize the asynchronous websocket client
    async with yf.AsyncWebSocket() as ws:
        # 3. Subscribe to one or more ticker symbols
        await ws.subscribe(symbols)
        
        # 4. Start listening for messages
        # Pass your custom handler to process the data as it arrives
        await ws.listen(message_handler=realtime_data_callback)
    return

# get instruments from Public API
def get_instruments():
    auth_token = get_auth_token(PUBLIC_API_KEY)
    headers = {
        "Authorization": build_bearer_token(auth_token),
        "Content-Type": "application/json",
        "User-Agent": "public-dev-docs",
        "typeFilter": "['EQUITY']",
        "tradingFilter": "['BUY_AND_SELL']"
    }
    response = requests.get(PUBLIC_API["getInstruments"], headers=headers)
    data = response.json()
    print("Instruments:\n\n", data)

def parse_quote(quote: dict) -> ParsedQuote:
    parsed_quote = ParsedQuote(quote)
    return parsed_quote

# returns a lst of ParsedQuotes
def scan_for_stocks():
    # PREDEFINED_SCREENER_QUERIES:
    # [
    #   'aggressive_small_caps', 
    #   'day_gainers', 
    #   'day_losers', 
    #   'growth_technology_stocks', 
    #   'most_actives', 
    #   'most_shorted_stocks', 
    #   'small_cap_gainers', 
    #   'undervalued_growth_stocks', 
    #   'undervalued_large_caps', 
    #   'conservative_foreign_funds', 
    #   'high_yield_bond', 
    #   'portfolio_anchors', 
    #   'solid_large_growth_funds', 
    #   'solid_midcap_growth_funds', 
    #   'top_mutual_funds', 
    #   'top_etfs_us', 
    #   'top_performing_etfs', 
    #   'technology_etfs', 
    #   'bond_etfs'
    #]
    screener_response = yf.screen("small_cap_gainers", count=100)
    results = []
    screener_quotes: list[dict] = screener_response["quotes"]
    results = [parse_quote(quote) for quote in screener_quotes]
    return results

def is_strong_buy(stock: ParsedQuote) -> bool:
    ANALYST_STRONG_BUY_INDICATOR = "Strong Buy"
    MIN_VOLUME = 1500000
    ANALYSIS_SEPARATOR = " - "
    def helper(stock: ParsedQuote):
        buy_criteria = {"strong_buy": False, "volume": 0}
        analyst_rating = stock.analyst_rating
        daily_volume = stock.volume
        if analyst_rating:
            #splits analyst_rating: '1.5 - Strong Buy' -> ['1.5', 'Strong Buy']
            metrics = analyst_rating.split(ANALYSIS_SEPARATOR)
            if len(metrics) > 1:
                buy_criteria.update({"strong_buy": metrics[1] == ANALYST_STRONG_BUY_INDICATOR})
                buy_criteria.update({"volume": daily_volume})
        return buy_criteria
    
    stock_metrics = helper(stock)
    return stock_metrics["strong_buy"] and stock_metrics["volume"] >= MIN_VOLUME



async def async_main():
    stocks_in_play = filter(is_strong_buy, scan_for_stocks())
    symbols_in_play: list[str] = [str(stock.symbol) if stock.symbol != None else '' for stock in stocks_in_play]
    await async_get_realtime_data(symbols_in_play)
    # return decision_model.perform_analysis(symbols_in_play)
    return stocks_in_play

if __name__ == "__main__":
    asyncio.run(async_main())

