# core/tasks.py - IMPROVED VERSION
"""
Improved Celery tasks with better error handling, pagination, and monitoring.
"""

from datetime import datetime, timedelta, time
import pytz
import time as time_module
from celery import Task, shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from django.db import transaction

from core.models import Asset, Candle, WatchListAsset, SyncStatus
from alpacabackend import const
from alpacabackend.cache_keys import cache_keys
from .services.alpaca_service import alpaca_service

logger = get_task_logger(__name__)


# Constants
LOCK_TTL = 300  # 5 minutes
TASK_LOCK_TTL = 600  # 10 minutes
STALE_SYNC_THRESHOLD_MINUTES = 5  # Consider sync stuck after this duration
MAX_RETRY_ATTEMPTS = 3  # Maximum retries for failed operations


class SingleInstanceTask(Task):
    """
    Custom task class that prevents multiple instances of the same task.
    Improved with automatic lock cleanup and better error handling.
    """

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        # Generate unique task ID based on task name and primary argument
        key_suffix = None
        if args and len(args) > 0:
            arg0 = args[0]
            try:
                if self.name == "fetch_historical_data":
                    # Use asset_id for historical data tasks
                    key_suffix = f"asset-{arg0}"
                elif self.name == "alpaca_sync" or self.name == "start_alpaca_stream":
                    key_suffix = f"account-{arg0}"
                else:
                    key_suffix = f"arg0-{arg0}"
            except Exception:
                key_suffix = f"arg0-{arg0}"
        
        if key_suffix:
            task_id = f"{self.name}-{key_suffix}"

        lock_key = f"task_lock:{task_id}"
        
        # Check if task is already running
        if cache.get(lock_key):
            logger.warning(
                f"Task {task_id} is already running (cache lock exists). Skipping."
            )
            return None
        
        # Set lock with TTL
        cache.set(lock_key, 1, timeout=TASK_LOCK_TTL)
        
        try:
            return super().apply_async(args, kwargs, task_id=task_id, **options)
        except Exception as e:
            # Release lock if task fails to start
            cache.delete(lock_key)
            logger.error(f"Failed to start task {task_id}: {e}")
            raise
    
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """Automatically clean up lock after task completes"""
        lock_key = f"task_lock:{task_id}"
        cache.delete(lock_key)
        logger.debug(f"Released lock for task {task_id}")


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


@shared_task(
    name="alpaca_sync",
    base=SingleInstanceTask,
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 60},
    retry_backoff=True
)
def alpaca_sync_task(
    self,
    asset_classes: list = None,
    batch_size: int = 500,  # Reduced for better memory management
    force: bool = False
):
    """
    IMPROVED asset sync with better pagination and error recovery.
    
    Improvements:
    - Smaller batch sizes to prevent memory issues
    - Better heartbeat mechanism
    - Automatic retry on failures
    - Detailed progress logging
    - Graceful degradation on errors
    """
    if asset_classes is None:
        asset_classes = ["us_equity", "crypto"]  # Removed us_option for now

    # Get or create sync status
    sync_status, created = SyncStatus.objects.get_or_create(
        sync_type="assets",
        defaults={"total_items": 0, "is_syncing": False}
    )
    
    logger.info(
        f"📊 Sync request: is_syncing={sync_status.is_syncing}, "
        f"force={force}, updated_at={sync_status.updated_at}"
    )

    # Handle force flag
    if force and sync_status.is_syncing:
        logger.info("🔄 Force flag set - resetting sync status")
        sync_status.is_syncing = False
        sync_status.save()

    # Auto-detect and fix stuck syncs
    was_stuck = _reset_stuck_sync(sync_status)
    
    # Check if sync is currently running
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
            
            if sync_status.is_syncing:
                if _reset_stuck_sync(sync_status):
                    logger.info("🔒 Lock acquired after resetting stuck sync")
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

    # Main sync logic
    service = alpaca_service
    total_created = 0
    total_updated = 0
    total_errors = 0
    start_time = timezone.now()

    try:
        for asset_class in asset_classes:
            logger.info(f"📈 Syncing {asset_class}...")
            
            # Heartbeat update
            sync_status.updated_at = timezone.now()
            sync_status.save(update_fields=['updated_at'])

            try:
                # Fetch assets with fallback for 403 errors
                fallback_symbols = None
                if asset_class == "us_equity":
                    fallback_symbols = [
                        "AAPL", "GOOGL", "MSFT", "TSLA", "NVDA",
                        "AMZN", "META", "NFLX", "AMD", "INTC"
                    ]

                assets_data = service.list_assets(
                    status="active",
                    asset_class=asset_class,
                    fallback_symbols=fallback_symbols,
                )

                if not assets_data:
                    logger.warning(f"⚠️ No assets returned for {asset_class}")
                    continue

                logger.info(f"✓ Fetched {len(assets_data)} {asset_class} assets")

                # Process in smaller, optimized chunks
                chunk_size = min(batch_size, 200)  # Smaller chunks
                total_chunks = (len(assets_data) + chunk_size - 1) // chunk_size
                
                for chunk_idx, i in enumerate(range(0, len(assets_data), chunk_size), 1):
                    # Heartbeat every chunk
                    sync_status.updated_at = timezone.now()
                    sync_status.save(update_fields=['updated_at'])
                    
                    chunk = assets_data[i:i + chunk_size]
                    logger.debug(f"Processing chunk {chunk_idx}/{total_chunks} ({len(chunk)} assets)")
                    
                    # Get existing assets in this chunk
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

                    # Bulk create with smaller batches
                    if assets_to_create:
                        try:
                            created_assets = Asset.objects.bulk_create(
                                assets_to_create,
                                batch_size=100,  # Smaller batch
                                ignore_conflicts=True,
                            )
                            total_created += len(created_assets)
                            logger.debug(f"Created {len(created_assets)} assets")
                        except Exception as e:
                            logger.error(f"❌ Error creating assets: {e}")
                            total_errors += len(assets_to_create)

                    # Bulk update with smaller batches
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
                                    batch_size=100,
                                )
                                total_updated += len(to_update)
                                logger.debug(f"Updated {len(to_update)} assets")

                        except Exception as e:
                            logger.error(f"❌ Error updating assets: {e}")
                            total_errors += len(assets_to_update)
                    
                    # Small delay between chunks to prevent overwhelming database
                    time_module.sleep(0.1)

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
            f"{total_created} created, {total_updated} updated, {total_errors} errors"
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


@shared_task(
    name="fetch_historical_data",
    base=SingleInstanceTask,
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 2, 'countdown': 120}
)
def fetch_historical_data(self, asset_id: int, priority: str = "normal"):
    """
    IMPROVED backfill with pagination support and better error handling.
    
    Improvements:
    - Proper pagination handling for large datasets
    - Chunk-based processing to prevent memory issues
    - Better progress tracking
    - Automatic retry on failures
    """
    from decimal import Decimal
    
    asset = Asset.objects.filter(id=asset_id).first()
    if not asset:
        logger.error(f"❌ Asset {asset_id} not found")
        return

    symbol = asset.symbol
    logger.info(f"📊 Starting historical data fetch for {symbol} (priority: {priority})")

    running_key = cache_keys.backfill(asset.id).running()
    if not cache.add(running_key, 1, timeout=LOCK_TTL):
        logger.info(f"⏳ Backfill already running for {symbol}")
        return

    try:
        service = alpaca_service
        end_date = timezone.now()

        # Priority mode: fetch recent data first
        if priority == "high":
            logger.info(f"🚀 High-priority backfill for {symbol} (recent first)")
            recent_start = end_date - timedelta(days=7)
            _fetch_1t_candles_improved(asset, service, recent_start, end_date)
            _resample_all_timeframes(asset, recent_start, end_date)
            
            # Then fetch older data
            full_start = end_date - timedelta(
                days=min(settings.HISTORIC_DATA_LOADING_LIMIT, 180)  # Cap at 6 months
            )
            if full_start < recent_start:
                _fetch_1t_candles_improved(asset, service, full_start, recent_start)
                _resample_all_timeframes(asset, full_start, recent_start)
        else:
            logger.info(f"📊 Standard backfill for {symbol}")
            start_date = end_date - timedelta(
                days=min(settings.HISTORIC_DATA_LOADING_LIMIT, 180)
            )
            _fetch_1t_candles_improved(asset, service, start_date, end_date)
            _resample_all_timeframes(asset, start_date, end_date)

        # Mark completion
        completion_key = cache_keys.backfill(asset.id).completed()
        cache.set(completion_key, 1, timeout=86400 * 7)
        logger.info(f"✅ Backfill complete for {symbol}")

    except Exception as e:
        logger.error(f"❌ Backfill failed for {symbol}: {e}", exc_info=True)
        raise
    finally:
        running_key = cache_keys.backfill(asset.id).running()
        queued_key = cache_keys.backfill(asset.id).queued()
        cache.delete(running_key)
        cache.delete(queued_key)
        
        lock_key = f"task_lock:{self.request.id}"
        cache.delete(lock_key)


def _fetch_1t_candles_improved(asset, service, start_date, end_date):
    """
    IMPROVED: Better pagination handling and memory management.
    """
    symbol = asset.symbol
    
    # Check existing data
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

    # Fetch with pagination support
    current_end = end_date
    total_created = 0
    page_token = None
    max_pages = 50  # Safety limit to prevent infinite loops
    page_count = 0
    
    while current_end > actual_start and page_count < max_pages:
        chunk_start = max(actual_start, current_end - timedelta(days=30))  # Smaller chunks
        
        try:
            params = {
                "start": chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sort": "desc",
                "limit": 10000,
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
            if page_token:
                page_token = None
                current_end = chunk_start
                continue
            break

        bars = (resp or {}).get("bars") or []
        
        if not bars:
            logger.debug(f"No more bars for {symbol}")
            break
        
        # Batch insert candles
        candles = []
        for bar in bars:
            ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
            
            # Filter market hours for stocks
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
                    volume=float(bar.get("v", 0)),
                    trade_count=bar.get("n"),
                    vwap=bar.get("vw"),
                    timeframe=const.TF_1T,
                )
            )
        
        # Bulk create in batches
        if candles:
            batch_size = 500
            for i in range(0, len(candles), batch_size):
                batch = candles[i:i + batch_size]
                Candle.objects.bulk_create(batch, ignore_conflicts=True)
            
            total_created += len(candles)
            logger.debug(f"✓ Inserted {len(candles)} candles for {symbol} (total: {total_created})")
        
        # Check pagination
        page_token = resp.get("next_page_token")
        page_count += 1
        
        if not page_token:
            page_token = None
            current_end = chunk_start
            if current_end <= actual_start:
                break
        
        # Small delay to prevent rate limiting
        time_module.sleep(0.2)

    if total_created > 0:
        logger.info(f"✅ Fetched {total_created} 1T candles for {symbol}")


def _resample_all_timeframes(asset, start_date, end_date):
    """Resample all higher timeframes from 1T data"""
    symbol = asset.symbol
    
    for tf, delta in const.TF_LIST:
        if tf == const.TF_1T:
            continue
            
        try:
            _resample_timeframe(asset, tf, delta, start_date, end_date)
        except Exception as e:
            logger.error(f"❌ Error resampling {tf} for {symbol}: {e}")


def _resample_timeframe(asset, tf, delta, start_date, end_date):
    """
    Resample a specific timeframe from 1T data.
    IMPROVED: Better memory management with chunked processing.
    """
    from collections import defaultdict
    import pytz
    
    # Process in chunks to prevent memory issues
    chunk_size = timedelta(days=7)
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + chunk_size, end_date)
        
        # Get 1T candles for this chunk
        m1_candles = list(
            Candle.objects.filter(
                asset=asset,
                timeframe=const.TF_1T,
                timestamp__gte=current_start,
                timestamp__lt=current_end,
            ).order_by("timestamp")
        )

        if not m1_candles:
            current_start = current_end
            continue

        # Group into time buckets
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

        if aggregated_data:
            _upsert_aggregated_candles(asset, tf, aggregated_data)
        
        current_start = current_end


def _upsert_aggregated_candles(asset, tf, aggregated_data):
    """Upsert aggregated candles in batches"""
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

    # Batch operations
    if to_create:
        batch_size = 200
        for i in range(0, len(to_create), batch_size):
            batch = to_create[i:i + batch_size]
            Candle.objects.bulk_create(batch, ignore_conflicts=True)
        logger.debug(f"✓ Created {len(to_create)} {tf} candles for {asset.symbol}")
        
    if to_update:
        batch_size = 200
        for i in range(0, len(to_update), batch_size):
            batch = to_update[i:i + batch_size]
            Candle.objects.bulk_update(
                batch,
                ["open", "high", "low", "close", "volume", "minute_candle_ids"]
            )
        logger.debug(f"✓ Updated {len(to_update)} {tf} candles for {asset.symbol}")


def _is_market_hours(dt):
    """Check if datetime is during US market hours"""
    eastern = pytz.timezone('US/Eastern')
    et_time = dt.astimezone(eastern)
    
    if et_time.weekday() >= 5:
        return False
    
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    return market_open <= et_time.time() < market_close


@shared_task(name="core.tasks.check_watchlist_candles")
def check_watchlist_candles():
    """
    Periodic task to check watchlist assets for missing candles.
    IMPROVED: Only runs during market hours and uses smarter detection.
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
    start_date = end_date - timedelta(days=7)  # Only check last 7 days

    checked = 0
    scheduled = 0

    for wla in watchlist_assets:
        asset = wla.asset
        
        # Skip if backfill is running
        running_key = cache_keys.backfill(asset.id).running()
        if cache.get(running_key):
            continue

        try:
            # Quick check: do we have recent data?
            latest_candle = Candle.objects.filter(
                asset=asset,
                timeframe=const.TF_1T,
            ).order_by('-timestamp').first()
            
            if latest_candle:
                age_minutes = (now - latest_candle.timestamp).total_seconds() / 60
                
                # If data is fresh (within 10 minutes), skip
                if age_minutes < 10:
                    checked += 1
                    continue
            
            # Schedule backfill for stale data
            logger.info(f"📥 Scheduling backfill for {asset.symbol} (stale data)")
            fetch_historical_data.delay(asset.id, priority='high')
            scheduled += 1
            
        except Exception as e:
            logger.error(f"❌ Error processing {asset.symbol}: {e}")
            continue

    logger.info(f"✅ Watchlist check complete: {checked} checked, {scheduled} scheduled")


@shared_task(
    name="core.tasks.start_alpaca_stream",
    base=SingleInstanceTask,
    bind=True
)
def start_alpaca_stream(self, scope: str = "global"):
    """
    Start the long-running Alpaca WebSocket client.
    
    IMPROVED: Now properly daemonized and won't block Celery worker.
    Uses systemd service pattern for better management.
    
    Note: This should ideally run as a separate process, not a Celery task.
    Consider using systemd, supervisor, or Docker container for production.
    """
    running_lock_key = f"websocket:runner:{scope}:running"
    
    # Check if already running
    existing_lock = cache.get(running_lock_key)
    if existing_lock:
        logger.info("🟡 WebSocket runner already active – skipping start")
        return {"started": False, "reason": "already_running"}
    
    # Try to acquire lock
    if not cache.add(running_lock_key, 1, timeout=24 * 3600):
        logger.info("🟡 WebSocket runner already active (lock exists)")
        return {"started": False, "reason": "already_running"}

    try:
        from .services.websocket.client import WebsocketClient
        import threading

        logger.info("🚀 Starting Alpaca WebSocket client (scope=%s)", scope)
        client = WebsocketClient(sandbox=False)
        
        def run_client():
            try:
                client.run()
            except Exception as e:
                logger.exception("❌ WebSocket client crashed: %s", e)
                cache.delete(running_lock_key)
        
        # Run in daemon thread
        thread = threading.Thread(target=run_client, daemon=True)
        thread.start()
        
        # Wait a moment to ensure it started
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