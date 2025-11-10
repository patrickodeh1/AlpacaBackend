# core/services/chart_updates.py
"""
Real-time Chart Update System

Provides WebSocket-based real-time chart updates to frontend clients.
Integrates with the existing WebSocket service to push price updates.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Optional
from django.core.cache import cache
from django.utils import timezone

from core.models import Asset, Candle

logger = logging.getLogger(__name__)


class ChartUpdateManager:
    """
    Manages real-time chart updates for subscribed clients.
    
    This is a lightweight layer that:
    1. Tracks which assets users are watching
    2. Aggregates price updates
    3. Broadcasts to subscribed clients via Django Channels
    
    Note: Requires Django Channels for WebSocket support.
    If not using Channels, can be adapted to use polling/SSE instead.
    """
    
    SUBSCRIPTION_TTL = 3600  # 1 hour
    
    @staticmethod
    def _make_subscription_key(user_id: int) -> str:
        """Generate cache key for user subscriptions"""
        return f"chart:subscriptions:user:{user_id}"
    
    @staticmethod
    def _make_price_key(asset_id: int) -> str:
        """Generate cache key for latest price"""
        return f"chart:price:{asset_id}"
    
    def subscribe_user(self, user_id: int, asset_ids: Set[int]):
        """
        Subscribe user to real-time updates for specific assets.
        
        Args:
            user_id: User ID
            asset_ids: Set of asset IDs to subscribe to
        """
        key = self._make_subscription_key(user_id)
        
        # Store subscriptions in cache
        cache.set(key, list(asset_ids), self.SUBSCRIPTION_TTL)
        
        logger.info(f"User {user_id} subscribed to {len(asset_ids)} assets")
    
    def unsubscribe_user(self, user_id: int):
        """Unsubscribe user from all chart updates"""
        key = self._make_subscription_key(user_id)
        cache.delete(key)
        
        logger.info(f"User {user_id} unsubscribed from chart updates")
    
    def get_subscriptions(self, user_id: int) -> Set[int]:
        """Get asset IDs user is subscribed to"""
        key = self._make_subscription_key(user_id)
        asset_ids = cache.get(key, [])
        return set(asset_ids)
    
    def update_price(self, asset_id: int, price: float, timestamp: datetime):
        """
        Update cached price for an asset.
        
        This is called by the WebSocket service when new ticks arrive.
        """
        key = self._make_price_key(asset_id)
        
        data = {
            'price': price,
            'timestamp': timestamp.isoformat(),
            'updated_at': timezone.now().isoformat()
        }
        
        # Cache for 30 seconds
        cache.set(key, data, 30)
    
    def get_latest_prices(self, asset_ids: Set[int]) -> Dict[int, Dict]:
        """
        Get latest prices for multiple assets.
        
        Returns:
            {asset_id: {'price': float, 'timestamp': str, 'updated_at': str}}
        """
        prices = {}
        
        for asset_id in asset_ids:
            key = self._make_price_key(asset_id)
            data = cache.get(key)
            
            if data:
                prices[asset_id] = data
        
        return prices
    
    def broadcast_price_update(
        self,
        asset_id: int,
        price: float,
        timestamp: datetime,
        channel_layer=None
    ):
        """
        Broadcast price update to all subscribed users.
        
        Args:
            asset_id: Asset ID
            price: Current price
            timestamp: Update timestamp
            channel_layer: Django Channels layer (optional)
        
        Note: If channel_layer is provided, will broadcast via WebSocket.
        Otherwise, just updates cache for polling clients.
        """
        # Update cache
        self.update_price(asset_id, price, timestamp)
        
        # If no channel layer, we're in polling mode
        if not channel_layer:
            return
        
        # Broadcast via WebSocket
        message = {
            'type': 'price_update',
            'asset_id': asset_id,
            'price': price,
            'timestamp': timestamp.isoformat()
        }
        
        # Send to asset-specific group
        try:
            from asgiref.sync import async_to_sync
            group_name = f"chart_asset_{asset_id}"
            async_to_sync(channel_layer.group_send)(
                group_name,
                {
                    'type': 'chart_update',
                    'message': message
                }
            )
            logger.debug(f"Broadcasted price update for asset {asset_id}: ${price}")
        except Exception as e:
            logger.error(f"Failed to broadcast price update: {e}")
    
    def get_recent_candles(
        self,
        asset: Asset,
        timeframe: str,
        limit: int = 100
    ) -> list:
        """
        Get recent candles for chart initialization.
        
        Returns list of OHLCV dicts for charting libraries.
        """
        candles = Candle.objects.filter(
            asset=asset,
            timeframe=timeframe
        ).order_by('-timestamp')[:limit]
        
        # Reverse to chronological order
        candles = list(reversed(candles))
        
        return [
            {
                'timestamp': c.timestamp.isoformat(),
                'open': float(c.open),
                'high': float(c.high),
                'low': float(c.low),
                'close': float(c.close),
                'volume': float(c.volume)
            }
            for c in candles
        ]
    
    def aggregate_tick_to_candle(
        self,
        asset_id: int,
        price: float,
        volume: float,
        timestamp: datetime,
        timeframe: str = '1T'
    ):
        """
        Aggregate real-time tick into current candle.
        
        This updates the "live" candle that's still forming.
        """
        # Round timestamp to timeframe boundary
        if timeframe == '1T':
            bucket = timestamp.replace(second=0, microsecond=0)
        elif timeframe == '5T':
            minute = (timestamp.minute // 5) * 5
            bucket = timestamp.replace(minute=minute, second=0, microsecond=0)
        # Add more timeframes as needed
        else:
            return None
        
        cache_key = f"chart:live_candle:{asset_id}:{timeframe}:{bucket.isoformat()}"
        
        # Get or initialize candle
        candle_data = cache.get(cache_key)
        
        if not candle_data:
            candle_data = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume,
                'timestamp': bucket.isoformat(),
                'is_live': True
            }
        else:
            # Update OHLCV
            candle_data['high'] = max(candle_data['high'], price)
            candle_data['low'] = min(candle_data['low'], price)
            candle_data['close'] = price
            candle_data['volume'] += volume
        
        # Cache for duration of candle period + 1 minute
        cache.set(cache_key, candle_data, 120)
        
        return candle_data


# Global instance
chart_manager = ChartUpdateManager()


# Integration hook for WebSocket service
def on_tick_received(asset_id: int, price: float, size: int, timestamp: datetime):
    """
    Called by WebSocket service when new tick arrives.
    
    Add this to your WebSocket message handler:
    
    from core.services.chart_updates import on_tick_received
    
    def on_message(msg):
        if msg['T'] == 't':  # Trade tick
            on_tick_received(
                asset_id=get_asset_id(msg['S']),
                price=msg['p'],
                size=msg['s'],
                timestamp=parse_timestamp(msg['t'])
            )
    """
    try:
        # Update price cache
        chart_manager.update_price(asset_id, price, timestamp)
        
        # Aggregate into live candle
        chart_manager.aggregate_tick_to_candle(
            asset_id=asset_id,
            price=price,
            volume=float(size),
            timestamp=timestamp,
            timeframe='1T'
        )
        
        # Broadcast to subscribers (if Django Channels available)
        try:
            from channels.layers import get_channel_layer
            channel_layer = get_channel_layer()
            if channel_layer:
                chart_manager.broadcast_price_update(
                    asset_id=asset_id,
                    price=price,
                    timestamp=timestamp,
                    channel_layer=channel_layer
                )
        except ImportError:
            # Django Channels not installed, skip WebSocket broadcast
            pass
            
    except Exception as e:
        logger.error(f"Error processing tick for chart updates: {e}")


# Polling API endpoint helper
def get_chart_updates_polling(user_id: int, since: Optional[datetime] = None) -> Dict:
    """
    Get chart updates for polling-based clients (no WebSocket).
    
    Returns:
        {
            'assets': {
                asset_id: {
                    'price': float,
                    'timestamp': str,
                    'change': float,
                    'change_pct': float
                }
            },
            'timestamp': str
        }
    
    Usage in view:
        @api_view(['GET'])
        def chart_updates(request):
            since = request.query_params.get('since')
            since_dt = parse_datetime(since) if since else None
            
            updates = get_chart_updates_polling(
                user_id=request.user.id,
                since=since_dt
            )
            
            return Response(updates)
    """
    # Get user's subscriptions
    asset_ids = chart_manager.get_subscriptions(user_id)
    
    if not asset_ids:
        return {
            'assets': {},
            'timestamp': timezone.now().isoformat()
        }
    
    # Get latest prices
    prices = chart_manager.get_latest_prices(asset_ids)
    
    # Enrich with change data if needed
    enriched = {}
    for asset_id, data in prices.items():
        enriched[asset_id] = {
            'price': data['price'],
            'timestamp': data['timestamp'],
            # TODO: Calculate change from previous close
            'change': 0.0,
            'change_pct': 0.0
        }
    
    return {
        'assets': enriched,
        'timestamp': timezone.now().isoformat()
    }