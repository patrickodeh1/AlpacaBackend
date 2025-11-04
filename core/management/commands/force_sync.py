from django.core.management.base import BaseCommand
from core.models import SyncStatus
from core.tasks import alpaca_sync_task

class Command(BaseCommand):
    help = 'Force sync (resets status first)'

    def handle(self, *args, **options):
        sync = SyncStatus.objects.filter(sync_type='assets').first()
        if sync and sync.is_syncing:
            self.stdout.write('Resetting stuck sync...')
            sync.is_syncing = False
            sync.save()
        
        self.stdout.write('Starting sync...')
        result = alpaca_sync_task.delay()
        self.stdout.write(self.style.SUCCESS(f'✓ Task queued: {result.id}'))