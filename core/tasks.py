from datetime import datetime, time, timedelta

from celery import Task, shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from django.db import transaction
import pytz

from core.models import (
    Asset,
    Candle,
    WatchListAsset,
)
from alpacabackend import const
from alpacabackend.cache_keys import cache_keys

from .services.alpaca_service import alpaca_service

logger = get_task_logger(__name__)


LOCK_TTL = 300  # 5 minutes
LOCK_WAIT = 5  # seconds to wait for lock
RETRY_MAX = None  # unlimited
RETRY_BACKOFF = True
RETRY_BACKOFF_MAX = 300  # cap 5 min

# Task lock TTL - prevent zombie locks
TASK_LOCK_TTL = 600  # 10 minutes


class SingleInstanceTask(Task):
    """Custom task class that prevents multiple instances of the same task"""

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        # Generate a unique task_id based on task + logical entity
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

        # Use cache-based locking instead of inspect for better reliability
        lock_key = f"task_lock:{task_id}"
        if cache.get(lock_key):
            logger.warning(
                f"Task {task_id} is already running (cache lock exists). Skipping new instance."
            )
            return None
        
        # Set lock with TTL to prevent zombie locks
        cache.set(lock_key, 1, timeout=TASK_LOCK_TTL)

        return super().apply_async(args, kwargs, task_id=task_id, **options)


@shared_task(name="alpaca_sync", base=SingleInstanceTask, bind=True)
def alpaca_sync_task(self, asset_classes: list = None, batch_size: int = 1000):
    """
    Optimized sync task that efficiently syncs assets from Alpaca API.
    Automatically detects and recovers from stuck syncs.

    Args:
        asset_classes: List of asset classes to sync (defaults to all)
        batch_size: Number of assets to process in each batch
    """
    if asset_classes is None:
        asset_classes = ["us_equity", "us_option", "crypto"]

    from core.models import SyncStatus

    # Get or create sync status
    sync_status, created = SyncStatus.objects.get_or_create(
        sync_type="assets", 
        defaults={"total_items": 0, "is_syncing": False}
    )
    
    logger.info(f"Sync request received. Current status: is_syncing={sync_status.is_syncing}, updated_at={sync_status.updated_at}")

    # Auto-detect and fix stale/stuck syncs (more than 5 minutes old)
    if sync_status.is_syncing:
        stale_threshold = timezone.now() - timedelta(minutes=5)
        is_stale = sync_status.updated_at < stale_threshold
        
        if is_stale:
            time_stuck = (timezone.now() - sync_status.updated_at).total_seconds() / 60
            logger.warning(
                f"Detected stale sync (stuck for {time_stuck:.1f} minutes). "
                f"Auto-recovering and starting fresh..."
            )
            sync_status.is_syncing = False
            sync_status.save()
        else:
            # Sync is actually running (updated recently)
            time_since_update = (timezone.now() - sync_status.updated_at).total_seconds()
            logger.warning(
                f"Sync already in progress (running for {time_since_update:.0f}s). "
                f"Please wait for it to complete."
            )
            return {
                "success": False, 
                "error": "Sync already in progress. Please wait.",
                "seconds_running": int(time_since_update)
            }

    # Mark as syncing with atomic transaction
    try:
        with transaction.atomic():
            # Re-fetch with lock to prevent race conditions
            sync_status = SyncStatus.objects.select_for_update().get(id=sync_status.id)
            
            # Double-check after lock
            if sync_status.is_syncing:
                time_since = (timezone.now() - sync_status.updated_at).total_seconds() / 60
                if time_since > 5:
                    logger.warning(f"Sync stuck for {time_since:.1f}m. Resetting...")
                    sync_status.is_syncing = False
                else:
                    logger.warning("Race condition: Sync started elsewhere")
                    return {"success": False, "error": "Sync already in progress"}
            
            sync_status.is_syncing = True
            sync_status.updated_at = timezone.now()
            sync_status.save()
            logger.info(f"✓ Acquired sync lock at {sync_status.updated_at}")
            
    except Exception as e:
        logger.error(f"Failed to acquire sync lock: {e}", exc_info=True)
        return {"success": False, "error": f"Database lock error: {str(e)}"}

    try:
        service = alpaca_service
        total_created = 0
        total_updated = 0
        total_errors = 0

        # Process each asset class
        for asset_class in asset_classes:
            logger.info(f"Starting sync for asset class: {asset_class}")
            
            # Update heartbeat
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
                    logger.warning(f"No assets returned for asset class: {asset_class}")
                    continue

                logger.info(f"Fetched {len(assets_data)} assets for {asset_class}")

                # Process in smaller chunks to avoid memory issues
                chunk_size = min(batch_size, 500)
                for i in range(0, len(assets_data), chunk_size):
                    # Update heartbeat every chunk
                    sync_status.updated_at = timezone.now()
                    sync_status.save(update_fields=['updated_at'])
                    
                    chunk = assets_data[i:i + chunk_size]
                    
                    # Get existing assets for this chunk
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

                    # Separate into create vs update
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
                            created_count = len(created_assets)
                            total_created += created_count
                            logger.info(f"Created {created_count} assets")
                        except Exception as e:
                            logger.error(f"Error creating assets: {e}")
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
                                updated_count = len(to_update)
                                total_updated += updated_count
                                logger.info(f"Updated {updated_count} assets")

                        except Exception as e:
                            logger.error(f"Error updating assets: {e}")
                            total_errors += len(assets_to_update)

                logger.info(
                    f"Completed {asset_class}: {total_created} created, "
                    f"{total_updated} updated"
                )

            except Exception as e:
                logger.error(f"Error syncing {asset_class}: {e}", exc_info=True)
                total_errors += 1
                continue

        result = {
            "success": True,
            "total_created": total_created,
            "total_updated": total_updated,
            "total_errors": total_errors,
            "asset_classes_processed": asset_classes,
        }

        # Update sync status on success
        sync_status.last_sync_at = timezone.now()
        sync_status.total_items = Asset.objects.filter(status="active").count()
        sync_status.is_syncing = False
        sync_status.save()

        logger.info(f"Asset sync completed: {result}")
        return result

    except Exception as e:
        error_msg = f"Critical error in alpaca_sync_task: {e}"
        logger.error(error_msg, exc_info=True)

        # Always mark sync as not running on error
        try:
            sync_status.is_syncing = False
            sync_status.save()
        except Exception as save_error:
            logger.error(f"Failed to reset sync status: {save_error}")

        return {"success": False, "error": error_msg}

    finally:
        # CRITICAL: Always ensure is_syncing is reset, no matter what
        try:
            # Re-fetch from database to get latest state
            fresh_status = SyncStatus.objects.filter(sync_type='assets').first()
            if fresh_status and fresh_status.is_syncing:
                fresh_status.is_syncing = False
                fresh_status.save()
                logger.info("✓ Reset is_syncing=False in finally block")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to reset sync status in finally: {e}")
            # Last resort: try without refresh
            try:
                SyncStatus.objects.filter(sync_type='assets').update(is_syncing=False)
                logger.info("✓ Force-reset is_syncing using update()")
            except Exception as e2:
                logger.error(f"CRITICAL: Complete failure to reset: {e2}")
        
        # Clear task lock
        try:
            lock_key = f"task_lock:{self.request.id}"
            cache.delete(lock_key)
        except Exception:
            pass  # Ignore cache errors


@shared_task(name="fetch_historical_data", base=SingleInstanceTask, bind=True)
def fetch_historical_data(self, asset_id: int):
    """
    Backfill strategy:
    1) Fetch only 1-minute bars from Alpaca and persist as 1T.
    2) For each higher timeframe (5T, 15T, 30T, 1H, 4H, 1D), resample from stored 1T
       to compute OHLCV and persist, storing the list of minute candle IDs used.
    """
    asset = Asset.objects.filter(id=asset_id).first()
    if not asset:
        logger.error(f"Asset with ID {asset_id} does not exist.")
        return

    symbol = asset.symbol

    running_key = cache_keys.backfill(asset.id).running()
    if not cache.add(running_key, 1, timeout=LOCK_TTL):
        logger.info(
            "Backfill already running for asset_id=%s symbol=%s — skipping.",
            asset.id,
            symbol,
        )
        return

    try:
        service = alpaca_service
        end_date = timezone.now()

        # Step 1: Fetch 1T only
        logger.info(f"Starting 1T candle fetch for {symbol}")
        try:
            last_1t = (
                Candle.objects.filter(asset=asset, timeframe=const.TF_1T)
                .order_by("-timestamp")
                .first()
            )
            start_date_1t = (
                last_1t.timestamp + timedelta(minutes=1)
                if last_1t
                else end_date - timedelta(days=settings.HISTORIC_DATA_LOADING_LIMIT)
            )

            if start_date_1t < end_date:
                # chunk by 10 days, but create newest first by walking backwards
                current_end, created_total = end_date, 0
                while current_end > start_date_1t:
                    r_start = max(start_date_1t, current_end - timedelta(days=10))
                    start_str = r_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                    end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    try:
                        resp = service.get_historic_bars(
                            symbol=symbol,
                            start=start_str,
                            end=end_str,
                            sort="desc",  # fetch latest first
                            asset_class=asset.asset_class,
                        )
                    except Exception as e:
                        logger.error(
                            f"API error for {symbol} 1T {r_start}->{current_end}: {e}",
                            exc_info=True,
                        )
                        current_end = r_start
                        continue

                    # Treat missing or null bars as no data (already up-to-date)
                    bars = (resp or {}).get("bars") or []
                    candles = []
                    for bar in bars:
                        ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
                        # optional filter market hours only
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
                                volume=int(bar.get("v", 0)),
                                trade_count=bar.get("n"),
                                vwap=bar.get("vw"),
                                timeframe=const.TF_1T,
                            )
                        )
                    if candles:
                        Candle.objects.bulk_create(candles, ignore_conflicts=True)
                        created_total += len(candles)
                    current_end = r_start
                logger.info(
                    f"Backfilled {created_total} 1T candles for {symbol}"
                )
            else:
                logger.info(f"1T candles for {symbol} are already up-to-date")
        except Exception as e:
            logger.error(f"Error backfilling 1T for {symbol}: {e}", exc_info=True)

        # Step 2: Resample from 1T to higher TFs and persist with linkage
        logger.info(f"Starting resampling for higher timeframes for {symbol}")
        for tf, delta in const.TF_LIST:
            if tf == const.TF_1T:
                continue
            try:
                logger.info(f"Processing {tf} timeframe for {symbol}")
                
                # Check if we have any 1T candles
                m1_count = Candle.objects.filter(
                    asset=asset, 
                    timeframe=const.TF_1T
                ).count()
                
                if m1_count == 0:
                    logger.warning(f"No 1T candles found for {symbol}, skipping {tf}")
                    continue
                
                logger.info(f"Found {m1_count} 1T candles for resampling to {tf}")

                # Get the last higher TF candle to determine where to start
                last_tf = (
                    Candle.objects.filter(asset=asset, timeframe=tf)
                    .order_by("-timestamp")
                    .first()
                )

                # Determine start bucket to (re)build
                # Start from the last TF candle + delta, or go back full history
                if last_tf:
                    start_ts = last_tf.timestamp + delta
                    logger.info(f"Resuming {tf} from {start_ts} (after last candle)")
                else:
                    start_ts = end_date - timedelta(days=settings.HISTORIC_DATA_LOADING_LIMIT)
                    logger.info(f"Starting fresh {tf} from {start_ts} (full history)")

                # Fetch 1T candles for resampling
                m1_candles = list(
                    Candle.objects.filter(
                        asset=asset,
                        timeframe=const.TF_1T,
                        timestamp__gte=start_ts,
                        timestamp__lt=end_date,
                    ).order_by("timestamp")
                )

                if not m1_candles:
                    logger.info(f"No 1T candles in range {start_ts} to {end_date} for {tf}")
                    continue

                logger.info(f"Found {len(m1_candles)} 1T candles to resample into {tf}")

                # Group candles into time buckets using Python (SQLite-compatible)
                from collections import defaultdict
                buckets = defaultdict(list)

                delta_seconds = int(delta.total_seconds())
                
                # Use market open as anchor for intraday timeframes
                eastern = pytz.timezone("US/Eastern")
                anchor_ts = datetime(1970, 1, 1, 9, 30, tzinfo=eastern).timestamp()

                for candle in m1_candles:
                    # Convert to Unix timestamp
                    ts_unix = candle.timestamp.timestamp()

                    # Calculate bucket start (aligned to market open for intraday)
                    if delta_seconds < 86400:  # Intraday timeframes
                        # Align to market open (9:30 ET)
                        days_since_epoch = int((ts_unix - anchor_ts) // 86400)
                        seconds_into_day = ts_unix - (anchor_ts + days_since_epoch * 86400)
                        bucket_offset = (seconds_into_day // delta_seconds) * delta_seconds
                        bucket_start = anchor_ts + (days_since_epoch * 86400) + bucket_offset
                    else:  # Daily timeframe
                        bucket_start = (ts_unix // delta_seconds) * delta_seconds

                    bucket_ts = datetime.fromtimestamp(bucket_start, tz=timezone.utc)
                    buckets[bucket_ts].append(candle)

                logger.info(f"Created {len(buckets)} buckets for {tf}")

                # Aggregate each bucket
                aggregated_data = []
                for bucket_ts, candles_in_bucket in sorted(buckets.items()):
                    if not candles_in_bucket:
                        continue

                    # Sort by timestamp for proper OHLC
                    sorted_candles = sorted(candles_in_bucket, key=lambda c: c.timestamp)

                    o = sorted_candles[0].open
                    h = max(c.high for c in sorted_candles)
                    l = min(c.low for c in sorted_candles)
                    c = sorted_candles[-1].close
                    v = sum(c.volume for c in sorted_candles)
                    ids = [c.id for c in sorted_candles]

                    aggregated_data.append((bucket_ts, o, h, l, c, v, ids))

                if not aggregated_data:
                    logger.warning(f"No aggregated data for {tf}")
                    continue

                logger.info(f"Aggregated {len(aggregated_data)} {tf} candles")

                # Upsert aggregated candles (create new + update existing)
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
                    logger.info(f"Created {len(to_create)} new {tf} candles for {symbol}")
                    
                if to_update:
                    Candle.objects.bulk_update(
                        to_update,
                        ["open", "high", "low", "close", "volume", "minute_candle_ids"],
                        batch_size=500,
                    )
                    logger.info(f"Updated {len(to_update)} existing {tf} candles for {symbol}")

            except Exception as e:
                logger.error(
                    f"Error resampling {tf} for {symbol}: {e}", exc_info=True
                )

        # Mark backfill as complete by setting a cache flag
        completion_key = cache_keys.backfill(asset.id).completed()
        cache.set(completion_key, 1, timeout=86400 * 7)  # Keep for 7 days
        logger.info(f"Marked backfill complete for asset_id={asset.id} symbol={symbol}")

    finally:
        # Always release the lock and clear any queued marker for this asset
        queued_key = cache_keys.backfill(asset.id).queued()
        cache.delete(running_key)
        cache.delete(queued_key)
        
        # Clear task lock
        lock_key = f"task_lock:{self.request.id}"
        cache.delete(lock_key)


@shared_task(name="check_watchlist_candles")
def check_watchlist_candles():
    """
    Periodic task to check watchlist assets for missing 1T candles in the last month.
    If missing candles are found, fetch them from Alpaca and resample higher timeframes.
    Only runs during North American market hours (9:30 AM - 4:00 PM ET, weekdays).
    """
    from django.utils import timezone

    now = timezone.now()

    # Check if it's market hours
    if not _is_market_hours(now):
        logger.info("Not market hours, skipping watchlist candle check")
        return

    logger.info("Starting watchlist candle check task")

    # Get all active watchlist assets
    watchlist_assets = (
        WatchListAsset.objects.filter(is_active=True, watchlist__is_active=True)
        .select_related("asset", "watchlist")
        .distinct("asset")
    )

    end_date = timezone.now()
    start_date = end_date - timedelta(days=30)  # Last 1 month

    total_missing_found = 0
    assets_processed = 0

    for wla in watchlist_assets:
        asset = wla.asset
        symbol = asset.symbol

        try:
            # Skip if backfill is currently running for this asset
            running_key = cache_keys.backfill(asset.id).running()
            try:
                if cache.get(running_key):
                    logger.info(f"Backfill running for {symbol}, skipping candle check")
                    continue
            except Exception as e:
                logger.warning(f"Failed to check backfill status for {symbol}: {e}")
                continue

            # Check for missing 1T candles in the last month
            missing_periods = _find_missing_candle_periods(asset, start_date, end_date)

            if not missing_periods:
                logger.debug(f"No missing candles for {symbol}")
                continue

            logger.info(f"Found {len(missing_periods)} missing periods for {symbol}")

            # Fetch missing data from Alpaca
            fetched_count = _fetch_missing_candles(asset, missing_periods)
            total_missing_found += fetched_count

            # Resample higher timeframes for the affected periods
            if fetched_count > 0:
                _resample_higher_timeframes(asset, start_date, end_date)

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}", exc_info=True)
            continue

        assets_processed += 1

    logger.info(
        f"Watchlist candle check completed. Processed {assets_processed} assets, "
        f"fetched {total_missing_found} missing candles"
    )


def _find_missing_candle_periods(asset, start_date, end_date):
    """
    Find periods where 1T candles are missing during market hours.
    Returns list of (start, end) tuples for missing periods.
    """
    missing_periods = []

    # Get all existing 1T candles in the period, ordered by timestamp
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
        # No candles at all, return the entire period
        return [(start_date, end_date)]

    # Check for gaps between consecutive candles
    prev_ts = None
    for ts in existing_candles:
        if prev_ts and _is_market_hours(prev_ts):
            # Calculate expected next timestamp (1 minute later)
            expected_next = prev_ts + timedelta(minutes=1)

            # If there's a gap and we're still in market hours, it's missing
            if expected_next < ts and _is_market_hours(expected_next):
                missing_periods.append((expected_next, ts))

        prev_ts = ts

    # Check if we need data before the first candle
    first_candle = existing_candles[0]
    if start_date < first_candle and _is_market_hours(start_date):
        missing_periods.append((start_date, first_candle))

    # Check if we need data after the last candle
    last_candle = existing_candles[-1]
    if last_candle < end_date and _is_market_hours(last_candle + timedelta(minutes=1)):
        missing_periods.append((last_candle + timedelta(minutes=1), end_date))

    return missing_periods


def _fetch_missing_candles(asset, missing_periods):
    """
    Fetch missing 1T candles from Alpaca for the given periods.
    Returns the number of candles fetched.
    """
    service = alpaca_service
    symbol = asset.symbol
    total_fetched = 0

    for start_period, end_period in missing_periods:
        try:
            # Fetch in chunks to avoid API limits
            current_end = end_period
            chunk_days = 7  # Fetch 1 week at a time

            while current_end > start_period:
                chunk_start = max(
                    start_period, current_end - timedelta(days=chunk_days)
                )

                start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")

                try:
                    resp = service.get_historic_bars(
                        symbol=symbol,
                        start=start_str,
                        end=end_str,
                        sort="desc",
                        timeframe=const.TF_1T,
                        asset_class=asset.asset_class,
                    )
                except Exception as e:
                    logger.error(
                        f"API error fetching {symbol} {chunk_start}->{current_end}: {e}"
                    )
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
                    logger.debug(
                        f"Fetched {len(candles)} candles for {symbol} in "
                        f"{chunk_start}->{current_end}"
                    )

                current_end = chunk_start

        except Exception as e:
            logger.error(
                f"Error fetching missing candles for {symbol}: {e}", exc_info=True
            )
            continue