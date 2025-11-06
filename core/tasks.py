from datetime import datetime, time, timedelta
import pytz
import time as time_module
from celery import Task, shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from django.db import transaction

from core.models import (
    Asset,
    Candle,
    WatchListAsset,
    SyncStatus,
)
from alpacabackend import const
from alpacabackend.cache_keys import cache_keys

from .services.alpaca_service import alpaca_service

logger = get_task_logger(__name__)


LOCK_TTL = 300  # 5 minutes
TASK_LOCK_TTL = 600  # 10 minutes
STALE_SYNC_THRESHOLD_MINUTES = 5  # Consider sync stuck after this duration


class SingleInstanceTask(Task):
    """Custom task class that prevents multiple instances of the same task"""

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        key_suffix = None
        if args and len(args) > 0:
            arg0 = args[0]
            try:
                if self.name == "fetch_historical_data":
                    wla = (
                        WatchListAsset.objects.filter(id=arg0)
                        .values("asset_id")
                        .first()
                    )
                    if wla:
                        key_suffix = f"asset-{wla['asset_id']}"
                    else:
                        key_suffix = f"watchlist-asset-{arg0}"
                elif self.name == "alpaca_sync" or self.name == "start_alpaca_stream":
                    key_suffix = f"account-{arg0}"
                else:
                    key_suffix = f"arg0-{arg0}"
            except Exception:
                key_suffix = f"arg0-{arg0}"
        if key_suffix:
            task_id = f"{self.name}-{key_suffix}"

        lock_key = f"task_lock:{task_id}"
        if cache.get(lock_key):
            logger.warning(
                f"Task {task_id} is already running (cache lock exists). Skipping."
            )
            return None
        
        cache.set(lock_key, 1, timeout=TASK_LOCK_TTL)
        return super().apply_async(args, kwargs, task_id=task_id, **options)


def _reset_stuck_sync(sync_status):
    """
    Detect and reset stuck syncs automatically.
    Returns True if sync was stuck and reset, False otherwise.
    """
    if not sync_status.is_syncing:
        return False
    
    stale_threshold = timezone.now() - timedelta(minutes=STALE_SYNC_THRESHOLD_MINUTES)
    is_stale = sync_status.updated_at < stale_threshold
    
    if is_stale:
        time_stuck = (timezone.now() - sync_status.updated_at).total_seconds() / 60
        logger.warning(
            f"🔧 Auto-recovering stuck sync (inactive for {time_stuck:.1f}m)"
        )
        sync_status.is_syncing = False
        sync_status.save()
        return True
    
    return False


@shared_task(name="alpaca_sync", base=SingleInstanceTask, bind=True)
def alpaca_sync_task(
    self, 
    asset_classes: list = None, 
    batch_size: int = 1000,
    force: bool = False  # New parameter to force reset
):
    """
    Optimized sync task with automatic recovery from stuck states.
    Safe to call directly from frontend - handles all edge cases gracefully.

    Args:
        asset_classes: List of asset classes to sync (defaults to all)
        batch_size: Number of assets to process in each batch
        force: If True, will reset any stuck sync and start fresh
    """
    if asset_classes is None:
        asset_classes = ["us_equity", "us_option", "crypto"]

    # Get or create sync status
    sync_status, created = SyncStatus.objects.get_or_create(
        sync_type="assets", 
        defaults={"total_items": 0, "is_syncing": False}
    )
    
    logger.info(
        f"📊 Sync request: is_syncing={sync_status.is_syncing}, "
        f"force={force}, updated_at={sync_status.updated_at}"
    )

    # Handle force flag - immediately reset if requested
    if force and sync_status.is_syncing:
        logger.info("🔄 Force flag set - resetting sync status")
        sync_status.is_syncing = False
        sync_status.save()

    # Auto-detect and fix stuck syncs
    was_stuck = _reset_stuck_sync(sync_status)
    
    # Check if sync is currently running (after auto-recovery attempt)
    if sync_status.is_syncing and not was_stuck:
        time_running = (timezone.now() - sync_status.updated_at).total_seconds()
        logger.warning(
            f"⏳ Sync already running ({time_running:.0f}s). "
            f"Use force=True to override."
        )
        return {
            "success": False, 
            "error": "Sync in progress",
            "seconds_running": int(time_running),
            "hint": "Wait or use force sync"
        }

    # Acquire sync lock with atomic transaction
    try:
        with transaction.atomic():
            sync_status = SyncStatus.objects.select_for_update().get(
                id=sync_status.id
            )
            
            # Final check after lock acquisition
            if sync_status.is_syncing:
                if _reset_stuck_sync(sync_status):
                    logger.info("🔓 Lock acquired after resetting stuck sync")
                else:
                    return {
                        "success": False, 
                        "error": "Sync started by another process"
                    }
            
            sync_status.is_syncing = True
            sync_status.updated_at = timezone.now()
            sync_status.save()
            logger.info(f"🔒 Sync lock acquired at {sync_status.updated_at}")
            
    except Exception as e:
        logger.error(f"❌ Failed to acquire sync lock: {e}", exc_info=True)
        return {"success": False, "error": f"Lock error: {str(e)}"}

    try:
        service = alpaca_service
        total_created = 0
        total_updated = 0
        total_errors = 0
        start_time = timezone.now()

        # Process each asset class
        for asset_class in asset_classes:
            logger.info(f"📈 Syncing {asset_class}...")
            
            # Heartbeat update
            sync_status.updated_at = timezone.now()
            sync_status.save(update_fields=['updated_at'])

            try:
                # Fetch assets from Alpaca
                fallback_symbols = (
                    ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
                    if asset_class == "us_equity"
                    else None
                )

                assets_data = service.list_assets(
                    status="active",
                    asset_class=asset_class,
                    fallback_symbols=fallback_symbols,
                )

                if not assets_data:
                    logger.warning(f"⚠️ No assets returned for {asset_class}")
                    continue

                logger.info(f"✓ Fetched {len(assets_data)} {asset_class} assets")

                # Process in optimized chunks
                chunk_size = min(batch_size, 500)
                for i in range(0, len(assets_data), chunk_size):
                    # Heartbeat every chunk
                    sync_status.updated_at = timezone.now()
                    sync_status.save(update_fields=['updated_at'])
                    
                    chunk = assets_data[i:i + chunk_size]
                    
                    # Get existing assets
                    chunk_alpaca_ids = [
                        asset_data.get("id", asset_data["symbol"]) 
                        for asset_data in chunk
                    ]
                    existing_alpaca_ids = set(
                        Asset.objects.filter(
                            alpaca_id__in=chunk_alpaca_ids,
                            asset_class=asset_class
                        ).values_list("alpaca_id", flat=True)
                    )

                    assets_to_create = []
                    assets_to_update = []

                    # Separate create vs update
                    for asset_data in chunk:
                        alpaca_id = asset_data.get("id", asset_data["symbol"])

                        asset_dict = {
                            "alpaca_id": alpaca_id,
                            "symbol": asset_data["symbol"],
                            "name": asset_data.get("name", ""),
                            "asset_class": asset_data.get("class", asset_class),
                            "exchange": asset_data.get("exchange"),
                            "status": asset_data.get("status", "active"),
                            "tradable": asset_data.get("tradable", False),
                            "marginable": asset_data.get("marginable", False),
                            "shortable": asset_data.get("shortable", False),
                            "easy_to_borrow": asset_data.get("easy_to_borrow", False),
                            "fractionable": asset_data.get("fractionable", False),
                            "maintenance_margin_requirement": asset_data.get(
                                "maintenance_margin_requirement"
                            ),
                            "margin_requirement_long": asset_data.get(
                                "margin_requirement_long"
                            ),
                            "margin_requirement_short": asset_data.get(
                                "margin_requirement_short"
                            ),
                        }

                        if alpaca_id in existing_alpaca_ids:
                            assets_to_update.append(asset_dict)
                        else:
                            assets_to_create.append(Asset(**asset_dict))

                    # Bulk create
                    if assets_to_create:
                        try:
                            created_assets = Asset.objects.bulk_create(
                                assets_to_create,
                                batch_size=200,
                                ignore_conflicts=True,
                            )
                            total_created += len(created_assets)
                        except Exception as e:
                            logger.error(f"❌ Error creating assets: {e}")
                            total_errors += len(assets_to_create)

                    # Bulk update
                    if assets_to_update:
                        try:
                            update_ids = [a["alpaca_id"] for a in assets_to_update]
                            existing = {
                                asset.alpaca_id: asset
                                for asset in Asset.objects.filter(
                                    alpaca_id__in=update_ids,
                                    asset_class=asset_class,
                                )
                            }

                            to_update = []
                            for asset_data in assets_to_update:
                                alpaca_id = asset_data["alpaca_id"]
                                if alpaca_id in existing:
                                    obj = existing[alpaca_id]
                                    for field, value in asset_data.items():
                                        if field != "alpaca_id":
                                            setattr(obj, field, value)
                                    to_update.append(obj)

                            if to_update:
                                Asset.objects.bulk_update(
                                    to_update,
                                    [
                                        "symbol", "name", "asset_class", "exchange",
                                        "status", "tradable", "marginable", "shortable",
                                        "easy_to_borrow", "fractionable",
                                        "maintenance_margin_requirement",
                                        "margin_requirement_long",
                                        "margin_requirement_short",
                                    ],
                                    batch_size=200,
                                )
                                total_updated += len(to_update)

                        except Exception as e:
                            logger.error(f"❌ Error updating assets: {e}")
                            total_errors += len(assets_to_update)

            except Exception as e:
                logger.error(f"❌ Error syncing {asset_class}: {e}", exc_info=True)
                total_errors += 1
                continue

        # Calculate duration
        duration = (timezone.now() - start_time).total_seconds()

        result = {
            "success": True,
            "total_created": total_created,
            "total_updated": total_updated,
            "total_errors": total_errors,
            "asset_classes_processed": asset_classes,
            "duration_seconds": round(duration, 2),
        }

        # Update sync status
        sync_status.last_sync_at = timezone.now()
        sync_status.total_items = Asset.objects.filter(status="active").count()
        sync_status.is_syncing = False
        sync_status.save()

        logger.info(
            f"✅ Sync complete in {duration:.1f}s: "
            f"{total_created} created, {total_updated} updated"
        )
        return result

    except Exception as e:
        error_msg = f"Critical sync error: {e}"
        logger.error(f"❌ {error_msg}", exc_info=True)

        try:
            sync_status.is_syncing = False
            sync_status.save()
        except Exception as save_error:
            logger.error(f"❌ Failed to reset status: {save_error}")

        return {"success": False, "error": error_msg}

    finally:
        # CRITICAL: Always reset is_syncing flag
        try:
            fresh_status = SyncStatus.objects.filter(sync_type='assets').first()
            if fresh_status and fresh_status.is_syncing:
                fresh_status.is_syncing = False
                fresh_status.save()
                logger.info("🔓 Released sync lock (finally block)")
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to reset in finally: {e}")
            try:
                SyncStatus.objects.filter(sync_type='assets').update(
                    is_syncing=False
                )
                logger.info("🔓 Force-reset via update()")
            except Exception as e2:
                logger.error(f"❌ CRITICAL: Complete failure: {e2}")
        
        # Clear task lock
        try:
            lock_key = f"task_lock:{self.request.id}"
            cache.delete(lock_key)
        except Exception:
            pass


@shared_task(name="force_sync_assets")
def force_sync_assets(asset_classes: list = None):
    """
    Convenience task that forces a sync by resetting any stuck state.
    Safe to call from frontend buttons.
    """
    return alpaca_sync_task.apply_async(
        kwargs={"asset_classes": asset_classes, "force": True}
    )


@shared_task(name="fetch_historical_data", base=SingleInstanceTask, bind=True)
def fetch_historical_data(self, asset_id: int, priority: str = "normal"):
    """
    Optimized backfill with priority support for faster chart loading.
    
    Args:
        asset_id: Asset ID to fetch data for
        priority: "high" for watchlist assets (load recent data first),
                  "normal" for background processing
    """
    asset = Asset.objects.filter(id=asset_id).first()
    if not asset:
        logger.error(f"❌ Asset {asset_id} not found")
        return

    symbol = asset.symbol

    running_key = cache_keys.backfill(asset.id).running()
    if not cache.add(running_key, 1, timeout=LOCK_TTL):
        logger.info(f"⏳ Backfill already running for {symbol}")
        return

    try:
        service = alpaca_service
        end_date = timezone.now()

        # Priority mode: fetch recent data first for faster chart display
        if priority == "high":
            logger.info(f"🚀 High-priority backfill for {symbol} (recent first)")
            # Fetch last 7 days first
            recent_start = end_date - timedelta(days=7)
            _fetch_1t_candles(asset, service, recent_start, end_date)
            _resample_all_timeframes(asset, recent_start, end_date)
            
            # Then fetch older data in background
            full_start = end_date - timedelta(
                days=settings.HISTORIC_DATA_LOADING_LIMIT
            )
            if full_start < recent_start:
                _fetch_1t_candles(asset, service, full_start, recent_start)
                _resample_all_timeframes(asset, full_start, recent_start)
        else:
            # Normal mode: fetch all data chronologically
            logger.info(f"📊 Standard backfill for {symbol}")
            start_date = end_date - timedelta(
                days=settings.HISTORIC_DATA_LOADING_LIMIT
            )
            _fetch_1t_candles(asset, service, start_date, end_date)
            _resample_all_timeframes(asset, start_date, end_date)

        # Mark completion
        completion_key = cache_keys.backfill(asset.id).completed()
        cache.set(completion_key, 1, timeout=86400 * 7)
        logger.info(f"✅ Backfill complete for {symbol}")

    finally:
        running_key = cache_keys.backfill(asset.id).running()
        queued_key = cache_keys.backfill(asset.id).queued()
        cache.delete(running_key)
        cache.delete(queued_key)
        
        lock_key = f"task_lock:{self.request.id}"
        cache.delete(lock_key)


def _fetch_1t_candles(asset, service, start_date, end_date):
    """Helper to fetch 1-minute candles from Alpaca with pagination support"""
    symbol = asset.symbol
    
    # Check if we already have data in this range
    last_1t = (
        Candle.objects.filter(
            asset=asset, 
            timeframe=const.TF_1T,
            timestamp__gte=start_date,
            timestamp__lt=end_date
        )
        .order_by("-timestamp")
        .first()
    )
    
    actual_start = (
        last_1t.timestamp + timedelta(minutes=1)
        if last_1t
        else start_date
    )

    if actual_start >= end_date:
        logger.debug(f"✓ {symbol} 1T data up-to-date")
        return

    # Fetch from beginning using pagination
    current_end = end_date
    total_created = 0
    page_token = None
    
    while current_end > actual_start:
        # For initial fetch, try to get as much as possible (10k limit)
        # For subsequent pages, use page_token
        chunk_start = max(actual_start, current_end - timedelta(days=365))  # 1 year chunks
        
        try:
            params = {
                "start": chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sort": "desc",
                "limit": 10000,  # Max allowed by Alpaca
            }
            if page_token:
                params["page_token"] = page_token
            
            resp = service.get_historic_bars(
                symbol=symbol,
                timeframe=const.TF_1T,
                start=params["start"],
                end=params["end"],
                limit=params["limit"],
                sort=params["sort"],
                page_token=params.get("page_token"),
                asset_class=asset.asset_class,
            )
        except Exception as e:
            logger.error(f"❌ API error for {symbol}: {e}")
            # If pagination fails, try smaller chunks
            if page_token:
                page_token = None
                current_end = chunk_start
                continue
            current_end = chunk_start
            continue

        bars = (resp or {}).get("bars") or []
        candles = []
        
        for bar in bars:
            ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
            # Only filter market hours for stocks, not crypto
            if asset.asset_class != "crypto" and not _is_market_hours(ts):
                continue
                
            candles.append(
                Candle(
                    asset=asset,
                    timestamp=ts,
                    open=float(bar["o"]),
                    high=float(bar["h"]),
                    low=float(bar["l"]),
                    close=float(bar["c"]),
                    volume=int(bar.get("v", 0)),
                    trade_count=bar.get("n"),
                    vwap=bar.get("vw"),
                    timeframe=const.TF_1T,
                )
            )
        
        if candles:
            Candle.objects.bulk_create(candles, ignore_conflicts=True)
            total_created += len(candles)
            logger.debug(f"✓ Fetched {len(candles)} candles for {symbol} (total: {total_created})")
        
        # Check for pagination
        page_token = resp.get("next_page_token")
        if not page_token:
            # No more pages, move to next time chunk
            page_token = None
            current_end = chunk_start
            if current_end <= actual_start:
                break
        # Continue with same time range but next page

    if total_created > 0:
        logger.info(f"✓ Fetched {total_created} 1T candles for {symbol} from {start_date.date()} to {end_date.date()}")


def _resample_all_timeframes(asset, start_date, end_date):
    """Helper to resample all higher timeframes from 1T data"""
    symbol = asset.symbol
    
    for tf, delta in const.TF_LIST:
        if tf == const.TF_1T:
            continue
            
        try:
            _resample_timeframe(asset, tf, delta, start_date, end_date)
        except Exception as e:
            logger.error(f"❌ Error resampling {tf} for {symbol}: {e}")


def _resample_timeframe(asset, tf, delta, start_date, end_date):
    """Resample a specific timeframe from 1T data"""
    from datetime import datetime
    import pytz
    
    # Get 1T candles for resampling
    m1_candles = list(
        Candle.objects.filter(
            asset=asset,
            timeframe=const.TF_1T,
            timestamp__gte=start_date,
            timestamp__lt=end_date,
        ).order_by("timestamp")
    )

    if not m1_candles:
        return

    # Group into time buckets
    from collections import defaultdict
    buckets = defaultdict(list)
    
    delta_seconds = int(delta.total_seconds())
    eastern = pytz.timezone("US/Eastern")
    anchor_ts = datetime(1970, 1, 1, 9, 30, tzinfo=eastern).timestamp()

    for candle in m1_candles:
        ts_unix = candle.timestamp.timestamp()

        if delta_seconds < 86400:  # Intraday
            days_since_epoch = int((ts_unix - anchor_ts) // 86400)
            seconds_into_day = ts_unix - (anchor_ts + days_since_epoch * 86400)
            bucket_offset = (seconds_into_day // delta_seconds) * delta_seconds
            bucket_start = anchor_ts + (days_since_epoch * 86400) + bucket_offset
        else:  # Daily
            bucket_start = (ts_unix // delta_seconds) * delta_seconds

        # Fix: Use pytz.UTC instead of timezone.utc
        bucket_ts = datetime.fromtimestamp(bucket_start, tz=pytz.UTC)
        buckets[bucket_ts].append(candle)

    # Aggregate buckets
    aggregated_data = []
    for bucket_ts, candles_in_bucket in sorted(buckets.items()):
        if not candles_in_bucket:
            continue

        sorted_candles = sorted(candles_in_bucket, key=lambda c: c.timestamp)
        
        aggregated_data.append((
            bucket_ts,
            sorted_candles[0].open,
            max(c.high for c in sorted_candles),
            min(c.low for c in sorted_candles),
            sorted_candles[-1].close,
            sum(c.volume for c in sorted_candles),
            [c.id for c in sorted_candles]
        ))

    if not aggregated_data:
        return

    # Upsert aggregated candles
    buckets_ts = [row[0] for row in aggregated_data]
    existing = {
        c.timestamp: c
        for c in Candle.objects.filter(
            asset=asset, timeframe=tf, timestamp__in=buckets_ts
        )
    }

    to_create = []
    to_update = []
    
    for bucket_ts, o, h, low_, c, v, ids in aggregated_data:
        if bucket_ts in existing:
            cobj = existing[bucket_ts]
            cobj.open = float(o)
            cobj.high = float(h)
            cobj.low = float(low_)
            cobj.close = float(c)
            cobj.volume = int(v or 0)
            cobj.minute_candle_ids = ids
            to_update.append(cobj)
        else:
            to_create.append(
                Candle(
                    asset=asset,
                    timeframe=tf,
                    timestamp=bucket_ts,
                    open=float(o),
                    high=float(h),
                    low=float(low_),
                    close=float(c),
                    volume=int(v or 0),
                    minute_candle_ids=ids,
                )
            )

    if to_create:
        Candle.objects.bulk_create(to_create, ignore_conflicts=True, batch_size=500)
        logger.debug(f"✓ Created {len(to_create)} {tf} candles for {asset.symbol}")
        
    if to_update:
        Candle.objects.bulk_update(
            to_update,
            ["open", "high", "low", "close", "volume", "minute_candle_ids"],
            batch_size=500,
        )
        logger.debug(f"✓ Updated {len(to_update)} {tf} candles for {asset.symbol}")


def _is_market_hours(dt):
    """Check if datetime is during US market hours (9:30 AM - 4:00 PM ET, weekdays)"""
    eastern = pytz.timezone('US/Eastern')
    et_time = dt.astimezone(eastern)
    
    # Check if weekday (Monday=0, Sunday=6)
    if et_time.weekday() >= 5:
        return False
    
    # Check if within market hours
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    return market_open <= et_time.time() < market_close


# Keep existing check_watchlist_candles and helper functions...
@shared_task(name="core.tasks.check_watchlist_candles")
def check_watchlist_candles():
    """
    Periodic task to check watchlist assets for missing candles.
    Now uses high-priority fetching for faster chart loading.
    """
    from django.utils import timezone

    now = timezone.now()

    if not _is_market_hours(now):
        logger.debug("Outside market hours, skipping watchlist check")
        return

    logger.info("🔍 Starting watchlist candle check")

    watchlist_assets = (
        WatchListAsset.objects.filter(is_active=True, watchlist__is_active=True)
        .select_related("asset", "watchlist")
        .distinct("asset")
    )

    end_date = timezone.now()
    start_date = end_date - timedelta(days=30)

    for wla in watchlist_assets:
        asset = wla.asset
        
        # Skip if backfill is running
        running_key = cache_keys.backfill(asset.id).running()
        if cache.get(running_key):
            continue

        try:
            missing_periods = _find_missing_candle_periods(
                asset, start_date, end_date
            )
            
            if missing_periods:
                logger.info(f"📥 Fetching missing data for {asset.symbol}")
                _fetch_missing_candles(asset, missing_periods)
                _resample_higher_timeframes(asset, start_date, end_date)
                
        except Exception as e:
            logger.error(f"❌ Error processing {asset.symbol}: {e}")
            continue

    logger.info("✅ Watchlist candle check complete")


def _find_missing_candle_periods(asset, start_date, end_date):
    """Find periods where 1T candles are missing"""
    missing_periods = []

    existing_candles = list(
        Candle.objects.filter(
            asset=asset,
            timeframe=const.TF_1T,
            timestamp__gte=start_date,
            timestamp__lt=end_date,
        )
        .order_by("timestamp")
        .values_list("timestamp", flat=True)
    )

    if not existing_candles:
        return [(start_date, end_date)]

    prev_ts = None
    for ts in existing_candles:
        if prev_ts and _is_market_hours(prev_ts):
            expected_next = prev_ts + timedelta(minutes=1)
            if expected_next < ts and _is_market_hours(expected_next):
                missing_periods.append((expected_next, ts))
        prev_ts = ts

    first_candle = existing_candles[0]
    if start_date < first_candle and _is_market_hours(start_date):
        missing_periods.append((start_date, first_candle))

    last_candle = existing_candles[-1]
    if last_candle < end_date and _is_market_hours(last_candle + timedelta(minutes=1)):
        missing_periods.append((last_candle + timedelta(minutes=1), end_date))

    return missing_periods


def _fetch_missing_candles(asset, missing_periods):
    """Fetch missing 1T candles from Alpaca"""
    service = alpaca_service
    symbol = asset.symbol
    total_fetched = 0

    for start_period, end_period in missing_periods:
        try:
            current_end = end_period
            chunk_days = 7

            while current_end > start_period:
                chunk_start = max(
                    start_period, current_end - timedelta(days=chunk_days)
                )

                try:
                    resp = service.get_historic_bars(
                        symbol=symbol,
                        start=chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        end=current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        sort="desc",
                        timeframe=const.TF_1T,
                        asset_class=asset.asset_class,
                    )
                except Exception as e:
                    logger.error(f"❌ API error for {symbol}: {e}")
                    current_end = chunk_start
                    continue

                bars = (resp or {}).get("bars") or []
                candles = []

                for bar in bars:
                    ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
                    if not _is_market_hours(ts):
                        continue

                    candles.append(
                        Candle(
                            asset=asset,
                            timestamp=ts,
                            open=float(bar["o"]),
                            high=float(bar["h"]),
                            low=float(bar["l"]),
                            close=float(bar["c"]),
                            volume=float(bar.get("v", 0)),
                            trade_count=bar.get("n"),
                            vwap=bar.get("vw"),
                            timeframe=const.TF_1T,
                        )
                    )

                if candles:
                    Candle.objects.bulk_create(candles, ignore_conflicts=True)
                    total_fetched += len(candles)

                current_end = chunk_start

        except Exception as e:
            logger.error(f"❌ Error fetching missing candles: {e}")
            continue

    return total_fetched


def _resample_higher_timeframes(asset, start_date, end_date):
    """Resample higher timeframes after fetching missing 1T data"""
    for tf, delta in const.TF_LIST:
        if tf == const.TF_1T:
            continue
        try:
            _resample_timeframe(asset, tf, delta, start_date, end_date)
        except Exception as e:
            logger.error(f"❌ Error resampling {tf}: {e}")


@shared_task(name="core.tasks.start_alpaca_stream", base=SingleInstanceTask, bind=True)
def start_alpaca_stream(self, scope: str = "global"):
    """
    Start the long-running Alpaca WebSocket client to ingest live data.

    Uses a cache-based global lock to prevent duplicate runners. Safe to call
    repeatedly via beat: it will no-op if already active.
    
    Note: This task runs in a daemon thread to avoid blocking Celery workers.
    """
    # Stronger lock than SingleInstanceTask TTL for long-running process
    # Use longer timeout since this is a long-running process
    running_lock_key = f"websocket:runner:{scope}:running"
    
    # Check if already running - extend lock if exists
    existing_lock = cache.get(running_lock_key)
    if existing_lock:
        logger.info("🟡 WebSocket runner already active — skipping start")
        return {"started": False, "reason": "already_running"}
    
    # Try to acquire lock with longer timeout
    if not cache.add(running_lock_key, 1, timeout=24 * 3600):
        logger.info("🟡 WebSocket runner already active (lock exists) — skipping start")
        return {"started": False, "reason": "already_running"}

    try:
        # Lazy import to avoid importing websocket client during Django app init/migrations
        from .services.websocket.client import WebsocketClient  # type: ignore
        import threading

        logger.info("🚀 Starting Alpaca WebSocket client (scope=%s)", scope)
        client = WebsocketClient(sandbox=False)
        
        # Run in a daemon thread so it doesn't block the Celery worker
        def run_client():
            try:
                client.run()
            except Exception as e:
                logger.exception("❌ WebSocket client crashed: %s", e)
                # Clear lock on crash so it can restart
                cache.delete(running_lock_key)
        
        thread = threading.Thread(target=run_client, daemon=True)
        thread.start()
        
        # Give it a moment to start, then return
        time_module.sleep(2)
        logger.info("✅ WebSocket client started in background thread")
        return {"started": True, "scope": scope}
        
    except ModuleNotFoundError as exc:
        logger.error("WebSocket dependencies missing: %s", exc)
        cache.delete(running_lock_key)
        return {"started": False, "error": str(exc)}
    except Exception as exc:
        logger.exception("❌ WebSocket runner crashed: %s", exc)
        cache.delete(running_lock_key)
        raise


@shared_task(name="core.tasks.cleanup_stuck_syncs")
def cleanup_stuck_syncs():
    """
    Periodic maintenance: detect and reset stuck SyncStatus entries.
    """
    try:
        status = SyncStatus.objects.filter(sync_type="assets").first()
        if not status:
            return {"updated": False}
        if _reset_stuck_sync(status):
            return {"updated": True}
        return {"updated": False}
    except Exception as exc:
        logger.error("Failed cleanup_stuck_syncs: %s", exc)
        return {"updated": False, "error": str(exc)}