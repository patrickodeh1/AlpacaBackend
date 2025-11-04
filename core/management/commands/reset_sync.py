from django.core.management.base import BaseCommand
from core.models import SyncStatus

class Command(BaseCommand):
    help = 'Reset stuck sync status'

    def add_arguments(self, parser):
        parser.add_argument('--sync-type', type=str, default='assets')

    def handle(self, *args, **options):
        sync_type = options['sync_type']
        sync_status = SyncStatus.objects.filter(sync_type=sync_type).first()
        
        if not sync_status:
            self.stdout.write(self.style.WARNING(f'No sync found: {sync_type}'))
            return
        
        was_syncing = sync_status.is_syncing
        sync_status.is_syncing = False
        sync_status.save()
        
        if was_syncing:
            self.stdout.write(self.style.SUCCESS(f'✓ Reset {sync_type} sync'))
        else:
            self.stdout.write(self.style.SUCCESS(f'✓ {sync_type} was not syncing'))