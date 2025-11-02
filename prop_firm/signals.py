from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from paper_trading.models import PaperTrade
from .models import PropFirmAccount, AccountActivity
import logging

logger = logging.getLogger(__name__)


@receiver(post_save, sender=PaperTrade)
def handle_trade_update(sender, instance, created, **kwargs):
    """
    Handle trade creation and updates to check rules and update balances
    """
    # Only process closed trades
    if instance.status != 'CLOSED':
        return
    
    # Find the active account for this user
    try:
        account = PropFirmAccount.objects.filter(
            user=instance.user,
            status='ACTIVE'
        ).latest('activated_at')
    except PropFirmAccount.DoesNotExist:
        return
    
    # Update account balance
    account.update_balance()
    
    # Check rules
    from .services.rule_engine import RuleEngine
    engine = RuleEngine(account)
    violations = engine.check_all_rules()
    
    # Log trade activity
    pnl = instance.realized_pl or 0
    pnl_str = f"+${abs(pnl):.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
    
    AccountActivity.objects.create(
        account=account,
        activity_type='TRADE_CLOSED',
        description=f"Trade closed: {instance.asset.symbol} {instance.direction} - P&L: {pnl_str}",
        metadata={
            'trade_id': instance.id,
            'symbol': instance.asset.symbol,
            'direction': instance.direction,
            'quantity': str(instance.quantity),
            'entry_price': str(instance.entry_price),
            'exit_price': str(instance.exit_price),
            'pnl': str(pnl)
        },
        created_by=instance.user
    )
    
    if violations:
        logger.warning(
            f"Account {account.account_number} has {len(violations)} violations "
            f"after closing trade {instance.id}"
        )


@receiver(post_save, sender=PaperTrade)
def handle_trade_creation(sender, instance, created, **kwargs):
    """
    Log when new trades are placed
    """
    if not created or instance.status != 'OPEN':
        return
    
    # Find the active account
    try:
        account = PropFirmAccount.objects.filter(
            user=instance.user,
            status='ACTIVE'
        ).latest('activated_at')
    except PropFirmAccount.DoesNotExist:
        return
    
    # Validate trade before allowing it
    from .services.rule_engine import TradeValidator
    
    validator = TradeValidator(account)
    can_trade, errors = validator.can_place_trade(
        asset=instance.asset,
        direction=instance.direction,
        quantity=instance.quantity,
        price=instance.entry_price
    )
    
    if not can_trade:
        logger.error(
            f"Invalid trade attempted for account {account.account_number}: {', '.join(errors)}"
        )
        # Note: In a real implementation, you'd want to prevent this at the API level
        # This is a backup check
        return
    
    # Log trade placement
    AccountActivity.objects.create(
        account=account,
        activity_type='TRADE_PLACED',
        description=f"Trade placed: {instance.asset.symbol} {instance.direction} @ ${instance.entry_price}",
        metadata={
            'trade_id': instance.id,
            'symbol': instance.asset.symbol,
            'direction': instance.direction,
            'quantity': str(instance.quantity),
            'entry_price': str(instance.entry_price)
        },
        created_by=instance.user
    )


@receiver(pre_save, sender=PropFirmAccount)
def handle_account_status_change(sender, instance, **kwargs):
    """
    Log account status changes
    """
    if not instance.pk:
        return
    
    try:
        old_instance = PropFirmAccount.objects.get(pk=instance.pk)
    except PropFirmAccount.DoesNotExist:
        return
    
    # Check if status changed
    if old_instance.status != instance.status:
        AccountActivity.objects.create(
            account=instance,
            activity_type='STATUS_CHANGE',
            description=f"Status changed from {old_instance.status} to {instance.status}",
            metadata={
                'old_status': old_instance.status,
                'new_status': instance.status
            }
        )
        
        logger.info(
            f"Account {instance.account_number} status changed: "
            f"{old_instance.status} -> {instance.status}"
        )