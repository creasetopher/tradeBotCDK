import yfinance as yf
from typing import Callable
from dataclasses import dataclass
import asyncio
import json


class ParsedQuote:
    def __init__(self, quote: dict[str, str]):

        def validate_quote(quoteAttribute: str | float | None, validation_fn = lambda x: x is not None and x != 0):
            # Implement any necessary validation logic for the quote data here
            # For example, you could check if the price is greater than 0, if the symbol is not empty, etc.
            if not validation_fn(quoteAttribute):
                error_msg = f"Invalid value for {quoteAttribute} in quote data"
                print(error_msg)
                raise ValueError(error_msg)        
            return quoteAttribute

        self.symbol = str(validate_quote(quote.get("symbol")))
        self.price = validate_quote(float(quote.get("regularMarketPrice", 0.0)))
        self.analyst_rating = quote.get("averageAnalystRating")
        self.volume = validate_quote(float(quote.get("regularMarketVolume", 0)))
        self.hi = validate_quote(float(quote.get("regularMarketDayHigh", 0.0)))
        self.low = validate_quote(float(quote.get("regularMarketDayLow", 0.0)))
        self.open = validate_quote(float(quote.get("regularMarketOpen", 0.0)))
        self.previous_close = validate_quote(float(quote.get("regularMarketPreviousClose", 0.0)))
        self.ema_200 = validate_quote(float(quote.get("twoHundredDayAverage", 0.0)))
        self.ema_50 = validate_quote(float(quote.get("fiftyDayAverage", 0.0)))
        self.ask = float(quote.get("ask", 0.0))
        self.bid = float(quote.get("bid", 0.0))
        self.short_name = validate_quote(quote.get("shortName", None))        

    def __str__(self):
        return json.dumps({
                "symbol": self.symbol,
                "price": self.price,
                "analyst_rating": self.analyst_rating,
                "volume": self.volume,
                "hi": self.hi,
                "low": self.low,
                "open": self.open,
                "previous_close": self.previous_close,
                "ema_200": self.ema_200,
                "ema_50": self.ema_50,
                "ask": self.ask,
                "bid": self.bid,
                "short_name": self.short_name,  
            })
    


class Utils:

    def __init__(self):
        return


    @staticmethod
    def _realtime_data_callback( message: str):
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
        print(f"Update received: {message}\n\n")

    @staticmethod
    def _parse_quote(quote: dict) -> ParsedQuote:
        parsed_quote = ParsedQuote(quote)
        return parsed_quote


    @staticmethod
    def scan_for_stocks():
        print("\nScanning for stocks...\n")
        scg_screener_response = yf.screen(YF_PREDEFINED_SCREENER_QUERIES.SMALL_CAP_GAINERS, count=100)
        dg_screener_response = yf.screen(YF_PREDEFINED_SCREENER_QUERIES.DAY_GAINERS, count=100)

        results = []
        small_cap_gainers_quotes: list[dict] = scg_screener_response["quotes"]
        day_gainers_quotes: list[dict] = dg_screener_response["quotes"]
        results = [Utils._parse_quote(quote) for quote in small_cap_gainers_quotes]
        results.extend([Utils._parse_quote(quote) for quote in day_gainers_quotes])

        return results

    @staticmethod
    def _is_strong_buy(stock: ParsedQuote) -> bool:
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

    

    @staticmethod
    async def async_get_realtime_data(symbols: list[str], callback: Callable[[str], None]):
        
        # 2. Initialize the asynchronous websocket client
        async with yf.AsyncWebSocket() as ws:
            # 3. Subscribe to one or more ticker symbols
            await ws.subscribe(symbols)
            
            # 4. Start listening for messages
            # Pass your custom handler to process the data as it arrives
            await ws.listen(message_handler=callback)
        return


    @classmethod
    async def async_fetch_ticker_data(cls, callback: Callable[[str], None] = _realtime_data_callback):
        stocks_in_play = filter(cls._is_strong_buy, cls.scan_for_stocks())
        symbols_in_play: list[str] = [str(stock.symbol) if stock.symbol != None else '' for stock in stocks_in_play]
        await cls.async_get_realtime_data(symbols_in_play, callback)
        # return decision_model.perform_analysis(symbols_in_play)
        return stocks_in_play



@dataclass
class YF_PREDEFINED_SCREENER_QUERIES:
    AGGRESSIVE_SMALL_CAPS: str = 'aggressive_small_caps'
    DAY_GAINERS: str = 'day_gainers'
    DAY_LOSERS: str = 'day_losers'
    GROWTH_TECHNOLOGY_STOCKS: str = 'growth_technology_stocks'
    MOST_ACTIVES: str = 'most_actives'
    MOST_SHORTED_STOCKS: str = 'most_shorted_stocks'
    SMALL_CAP_GAINERS: str = 'small_cap_gainers'
    UNDERVALUED_GROWTH_STOCKS: str = 'undervalued_growth_stocks'
    UNDERVALUED_LARGE_CAPS: str = 'undervalued_large_caps'
    CONSERVATIVE_FOREIGN_FUNDS: str = 'conservative_foreign_funds'
    HIGH_YIELD_BOND: str = 'high_yield_bond'
    PORTFOLIO_ANCHORS: str = 'portfolio_anchors'
    SOLID_LARGE_GROWTH_FUNDS: str = 'solid_large_growth_funds'
    SOLID_MIDCAP_GROWTH_FUNDS: str = 'solid_midcap_growth_funds'
    TOP_MUTUAL_FUNDS: str = 'top_mutual_funds'
    TOP_ETFS_US: str = 'top_etfs_us'
    TOP_PERFORMING_ETFS: str = 'top_performing_etfs'
    TECHNOLOGY_ETFS: str = 'technology_etfs'
    BOND_ETFS: str = 'bond_etfs'