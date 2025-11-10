# core/services/market_data_cache.py
"""
Market Data Caching Service

Provides intelligent caching for market data to reduce database queries
and Alpaca API calls while ensuring data freshness.
"""

import logging
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from django.core.cache import cache
from django.utils import timezone

from core.models import Asset, Candle, Tick
from core.services.alpaca_service import alpaca_service

logger = logging.getLogger(__name__)


class MarketDataCache:
    """
    Intelligent caching layer for market data.
    
    Cache Strategy:
    - Current prices: 5 second TTL (hot data)
    - Recent candles: 60 second TTL (warm data)
    - Historical candles: 5 minute TTL (cold data)
    - Asset metadata: 1 hour TTL (static data)
    """
    
    # Cache TTLs in seconds
    PRICE_TTL = 5           # Current price cache
    RECENT_CANDLE_TTL = 60  # Last few candles
    HISTORICAL_TTL = 300    # Older candles
    ASSET_TTL = 3600        # Asset metadata
    
    @staticmethod
    def _make_key(prefix: str, *args) -> str:
        """Generate cache key"""
        parts = [str(arg) for arg in args]
        return f"market:{prefix}:{':'.join(parts)}"
    
    def get_current_price(self, asset: Asset) -> Optional[Decimal]:
        """
        Get current market price with intelligent caching.
        
        Priority:
        1. Cache (5s TTL)
        2. Recent tick from DB (last 5 minutes)
        3. Latest candle from DB
        4. Live API call
        """
        cache_key = self._make_key("price", asset.id)
        
        # Try cache first
        cached_price = cache.get(cache_key)
        if cached_price is not None:
            logger.debug(f"Cache hit for {asset.symbol} price: ${cached_price}")
            return Decimal(str(cached_price))
        
        # Try recent tick
        recent_tick = Tick.objects.filter(
            asset=asset,
            timestamp__gte=timezone.now() - timedelta(minutes=5)
        ).order_by('-timestamp').first()
        
        if recent_tick:
            price = Decimal(str(recent_tick.price))
            cache.set(cache_key, float(price), self.PRICE_TTL)
            return price
        
        # Try latest candle
        latest_candle = Candle.objects.filter(
            asset=asset,
            timeframe='1T'
        ).order_by('-timestamp').first()
        
        if latest_candle:
            price = Decimal(str(latest_candle.close))
            cache.set(cache_key, float(price), self.PRICE_TTL)
            return price
        
        # Fallback to API
        try:
            end = datetime.now()
            start = end - timedelta(days=1)
            
            response = alpaca_service.get_historic_bars(
                symbol=asset.symbol,
                timeframe='1T',
                start=start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                end=end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                limit=1,
                sort='desc',
                asset_class=asset.asset_class
            )
            
            bars = response.get('bars', [])
            if bars:
                price = Decimal(str(bars[0]['c']))
                cache.set(cache_key, float(price), self.PRICE_TTL)
                logger.info(f"Fetched live price from API for {asset.symbol}: ${price}")
                return price
                
        except Exception as e:
            logger.error(f"Failed to fetch price from API for {asset.symbol}: {e}")
        
        return None
    
    def get_candles(
        self,
        asset: Asset,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Get candles with intelligent caching.
        
        Caches recent data aggressively, historical data less so.
        """
        # Determine if this is recent or historical data
        now = timezone.now()
        is_recent = (now - end).total_seconds() < 3600  # Within last hour
        
        cache_ttl = self.RECENT_CANDLE_TTL if is_recent else self.HISTORICAL_TTL
        cache_key = self._make_key(
            "candles",
            asset.id,
            timeframe,
            start.isoformat(),
            end.isoformat(),
            limit or 'all'
        )
        
        # Try cache
        cached = cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Cache hit for {asset.symbol} {timeframe} candles")
            return cached
        
        # Fetch from database
        queryset = Candle.objects.filter(
            asset=asset,
            timeframe=timeframe,
            timestamp__gte=start,
            timestamp__lt=end
        ).order_by('-timestamp')
        
        if limit:
            queryset = queryset[:limit]
        
        candles = [
            {
                'timestamp': c.timestamp.isoformat(),
                'open': float(c.open),
                'high': float(c.high),
                'low': float(c.low),
                'close': float(c.close),
                'volume': float(c.volume),
            }
            for c in queryset
        ]
        
        # Cache the results
        if candles:
            cache.set(cache_key, candles, cache_ttl)
            logger.debug(f"Cached {len(candles)} candles for {asset.symbol}")
        
        return candles
    
    def get_multiple_prices(self, asset_ids: List[int]) -> Dict[int, Decimal]:
        """
        Get current prices for multiple assets efficiently.
        
        Uses batch fetching and caching to minimize queries.
        """
        prices = {}
        uncached_ids = []
        
        # Check cache for each asset
        for asset_id in asset_ids:
            cache_key = self._make_key("price", asset_id)
            cached_price = cache.get(cache_key)
            
            if cached_price is not None:
                prices[asset_id] = Decimal(str(cached_price))
            else:
                uncached_ids.append(asset_id)
        
        if not uncached_ids:
            return prices
        
        # Batch fetch from database
        latest_candles = Candle.objects.filter(
            asset_id__in=uncached_ids,
            timeframe='1T'
        ).order_by('asset_id', '-timestamp').distinct('asset_id')
        
        for candle in latest_candles:
            price = Decimal(str(candle.close))
            prices[candle.asset_id] = price
            
            # Cache it
            cache_key = self._make_key("price", candle.asset_id)
            cache.set(cache_key, float(price), self.PRICE_TTL)
        
        return prices
    
    def invalidate_price(self, asset: Asset):
        """Invalidate cached price for an asset"""
        cache_key = self._make_key("price", asset.id)
        cache.delete(cache_key)
        logger.debug(f"Invalidated price cache for {asset.symbol}")
    
    def invalidate_candles(self, asset: Asset, timeframe: str):
        """
        Invalidate cached candles for an asset/timeframe.
        
        Note: This uses pattern-based deletion which may not work
        with all cache backends. For production, consider using
        cache versioning instead.
        """
        pattern = self._make_key("candles", asset.id, timeframe, "*")
        # This requires cache backend support (Redis)
        try:
            cache.delete_pattern(pattern)
            logger.debug(f"Invalidated candle cache for {asset.symbol} {timeframe}")
        except AttributeError:
            logger.warning(f"Cache backend doesn't support pattern deletion")
    
    def warm_cache_for_watchlist(self, asset_ids: List[int]):
        """
        Pre-warm cache with prices for watchlist assets.
        
        Call this periodically for frequently accessed assets.
        """
        logger.info(f"Warming cache for {len(asset_ids)} assets")
        
        assets = Asset.objects.filter(id__in=asset_ids)
        
        for asset in assets:
            try:
                self.get_current_price(asset)
            except Exception as e:
                logger.error(f"Failed to warm cache for {asset.symbol}: {e}")
    
    def get_ohlcv_summary(
        self,
        asset: Asset,
        period: str = '1D'
    ) -> Optional[Dict]:
        """
        Get OHLCV summary for a period (1D, 1W, 1M, 1Y).
        
        Returns: {
            'open': float,
            'high': float, 
            'low': float,
            'close': float,
            'volume': float,
            'change': float,
            'change_pct': float
        }
        """
        cache_key = self._make_key("ohlcv", asset.id, period)
        
        # Try cache
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        # Calculate period
        now = timezone.now()
        period_map = {
            '1D': timedelta(days=1),
            '1W': timedelta(weeks=1),
            '1M': timedelta(days=30),
            '1Y': timedelta(days=365),
        }
        
        delta = period_map.get(period)
        if not delta:
            return None
        
        start = now - delta
        
        # Fetch candles
        candles = Candle.objects.filter(
            asset=asset,
            timeframe='1T',
            timestamp__gte=start
        ).order_by('timestamp')
        
        if not candles:
            return None
        
        # Calculate summary
        first = candles.first()
        last = candles.last()
        
        summary = {
            'open': float(first.open),
            'high': float(max(c.high for c in candles)),
            'low': float(min(c.low for c in candles)),
            'close': float(last.close),
            'volume': float(sum(c.volume for c in candles)),
            'change': float(last.close - first.open),
            'change_pct': float((last.close - first.open) / first.open * 100) if first.open else 0
        }
        
        # Cache for 5 minutes
        cache.set(cache_key, summary, 300)
        
        return summary


# Global instance
market_data_cache = MarketDataCache()


# Convenience functions
def get_cached_price(asset: Asset) -> Optional[Decimal]:
    """Get current price with caching"""
    return market_data_cache.get_current_price(asset)


def get_cached_candles(
    asset: Asset,
    timeframe: str,
    start: datetime,
    end: datetime,
    limit: Optional[int] = None
) -> List[Dict]:
    """Get candles with caching"""
    return market_data_cache.get_candles(asset, timeframe, start, end, limit)


def get_multiple_cached_prices(asset_ids: List[int]) -> Dict[int, Decimal]:
    """Get prices for multiple assets with caching"""
    return market_data_cache.get_multiple_prices(asset_ids)