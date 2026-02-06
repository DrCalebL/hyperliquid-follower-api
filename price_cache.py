"""
Nike Rocket - Price Cache
=========================
Simple time-based cache for market prices to reduce API calls.

Usage:
    from price_cache import price_cache, get_cached_price
    
    # Get price (uses cache if available)
    price = await get_cached_price(exchange, "BTC")

Author: Nike Rocket Team
"""
import time
import asyncio
from dataclasses import dataclass
from typing import Dict, Optional
import logging

logger = logging.getLogger("PRICE_CACHE")


@dataclass
class CachedPrice:
    """Cached price with timestamp"""
    price: float
    timestamp: float


class PriceCache:
    """
    Simple time-based cache for market prices.
    
    Default TTL is 5 seconds - prices older than this are refetched.
    """
    
    def __init__(self, ttl_seconds: int = 5):
        self.cache: Dict[str, CachedPrice] = {}
        self.ttl = ttl_seconds
    
    def get(self, symbol: str) -> Optional[float]:
        """Get cached price if still valid"""
        if symbol in self.cache:
            cached = self.cache[symbol]
            age = time.time() - cached.timestamp
            if age < self.ttl:
                return cached.price
            # Expired, remove it
            del self.cache[symbol]
        return None
    
    def set(self, symbol: str, price: float):
        """Cache a price"""
        self.cache[symbol] = CachedPrice(price=price, timestamp=time.time())
    
    def invalidate(self, symbol: str = None):
        """Clear cache for a symbol or all symbols"""
        if symbol:
            self.cache.pop(symbol, None)
        else:
            self.cache.clear()
    
    def stats(self) -> dict:
        """Get cache statistics"""
        now = time.time()
        valid = sum(1 for c in self.cache.values() if now - c.timestamp < self.ttl)
        return {
            "total_entries": len(self.cache),
            "valid_entries": valid,
            "ttl_seconds": self.ttl
        }


# Global cache instance (5 second TTL)
price_cache = PriceCache(ttl_seconds=5)


async def get_cached_price(exchange, symbol: str) -> float:
    """
    Get price with caching.
    
    Checks cache first, fetches from exchange if cache miss or expired.
    
    Args:
        exchange: CCXT exchange instance
        symbol: Trading symbol (e.g., "BTC")
    
    Returns:
        Current price as float
    """
    # Check cache
    cached = price_cache.get(symbol)
    if cached is not None:
        logger.debug(f"Cache HIT: {symbol} = ${cached:.2f}")
        return cached
    
    # Cache miss - fetch from exchange
    logger.debug(f"Cache MISS: {symbol} - fetching from exchange")
    ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
    price = float(ticker['last'])
    
    # Store in cache
    price_cache.set(symbol, price)
    
    return price


async def get_cached_prices(exchange, symbols: list) -> Dict[str, float]:
    """
    Get multiple prices with caching.
    
    Batches API calls for symbols not in cache.
    
    Args:
        exchange: CCXT exchange instance
        symbols: List of trading symbols
    
    Returns:
        Dict of symbol -> price
    """
    results = {}
    to_fetch = []
    
    # Check cache for each symbol
    for symbol in symbols:
        cached = price_cache.get(symbol)
        if cached is not None:
            results[symbol] = cached
        else:
            to_fetch.append(symbol)
    
    # Fetch missing prices
    if to_fetch:
        # Use fetch_tickers if available (single API call)
        try:
            tickers = await asyncio.to_thread(exchange.fetch_tickers, to_fetch)
            for symbol, ticker in tickers.items():
                if symbol in to_fetch:
                    price = float(ticker['last'])
                    results[symbol] = price
                    price_cache.set(symbol, price)
        except Exception:
            # Fall back to individual fetches
            for symbol in to_fetch:
                try:
                    ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
                    price = float(ticker['last'])
                    results[symbol] = price
                    price_cache.set(symbol, price)
                except Exception as e:
                    logger.warning(f"Could not fetch price for {symbol}: {e}")
    
    return results
