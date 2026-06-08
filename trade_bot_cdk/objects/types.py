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
