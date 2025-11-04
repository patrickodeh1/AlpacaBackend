from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from core.models import SyncStatus

class Command(BaseCommand):
    help = 'Check current sync status'

    def handle(self, *args, **options):
        self.stdout.write("\nSYNC STATUS:")
        for sync in SyncStatus.objects.all():
            status = "🔄 SYNCING" if sync.is_syncing else "✓ IDLE"
            self.stdout.write(f"\n{sync.sync_type}: {status}")
            self.stdout.write(f"  Items: {sync.total_items:,}")
            
            if sync.last_sync_at:
                ago = (timezone.now() - sync.last_sync_at).total_seconds() / 3600
                self.stdout.write(f"  Last: {ago:.1f}h ago")
            
            if sync.is_syncing:
                mins = (timezone.now() - sync.updated_at).total_seconds() / 60
                self.stdout.write(f"  Running: {mins:.1f}m")
                if mins > 10:
                    self.stdout.write(self.style.ERROR("  ⚠️  STUCK!"))