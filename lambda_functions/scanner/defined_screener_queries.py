from __future__ import annotations
from dataclasses import dataclass

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