import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alpacabackend.settings')

app = Celery('alpacabackend')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Fix Celery 6.0 deprecation warning
app.conf.broker_connection_retry_on_startup = True


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
    
from celery.schedules import crontab

# Configure beat schedule via app.conf to ensure Celery picks it up
# Use full module path to match task registration
app.conf.beat_schedule = {
    'cleanup-stuck-syncs': {
        'task': 'core.tasks.cleanup_stuck_syncs',
        'schedule': crontab(minute='*/15'),  # Every 15 minutes
    },
    'start-websocket-runner': {
        'task': 'core.tasks.start_alpaca_stream',
        'schedule': crontab(minute='*/5'),  # Check every 5 minutes; task no-ops if running
        'args': ("global",),
    },
    'check-watchlist-candles': {
        'task': 'core.tasks.check_watchlist_candles',
        'schedule': crontab(minute='*/1'),  # Every minute; task skips outside RTH
    },
}

# Ensure task names are properly registered
app.conf.task_routes = {
    'core.tasks.*': {'queue': 'celery'},
}