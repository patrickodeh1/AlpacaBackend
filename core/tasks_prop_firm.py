from celery import shared_task
from celery.utils.log import get_task_logger
from django.db import transaction
from decimal import Decimal

from prop_firm.models import PropFirmAccount
from core.services.alpaca_service import alpaca_service

logger = get_task_logger(__name__)

@shared_task
def sync_alpaca_account_balances():
    """
    Sync Alpaca paper trading account balances with prop firm accounts.
    """
    accounts = PropFirmAccount.objects.filter(
        status='ACTIVE',
        alpaca_account_id__isnull=False
    )
    
    for account in accounts:
        try:
            # Get latest Alpaca account details
            alpaca_account = alpaca_service.get_account(account.alpaca_account_id)
            
            if alpaca_account:
                with transaction.atomic():
                    account.alpaca_account_status = alpaca_account.get('status')
                    account.alpaca_buying_power = Decimal(str(alpaca_account.get('buying_power', '0')))
                    account.current_balance = Decimal(str(alpaca_account.get('equity', '0')))
                    
                    # Update high water mark if current balance is higher
                    if account.current_balance > account.high_water_mark:
                        account.high_water_mark = account.current_balance
                    
                    # Calculate losses
                    if account.current_balance < account.starting_balance:
                        account.total_loss = account.starting_balance - account.current_balance
                    
                    account.save()
                    logger.info(f"Updated account {account.account_number} balance to ${account.current_balance}")
                    
        except Exception as e:
            logger.error(f"Error syncing account {account.account_number}: {str(e)}")
            continue