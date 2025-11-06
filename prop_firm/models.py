import logging
from decimal import Decimal
from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone
from django.db.models import Sum

logger = logging.getLogger(__name__)


class PropFirmPlan(models.Model):
    """Different account tiers users can purchase"""
    
    PLAN_TYPE_CHOICES = [
        ('EVALUATION', 'Evaluation'),
        ('FUNDED', 'Funded'),
    ]
    
    name = models.CharField(max_length=100)  # e.g., "$50K Challenge"
    description = models.TextField()
    plan_type = models.CharField(max_length=20, choices=PLAN_TYPE_CHOICES, default='EVALUATION')
    
    # Account Parameters
    starting_balance = models.DecimalField(max_digits=12, decimal_places=2)
    price = models.DecimalField(max_digits=10, decimal_places=2)  # Purchase price
    
    # Trading Rules
    max_daily_loss = models.DecimalField(
        max_digits=12, 
        decimal_places=2,
        help_text="Maximum loss allowed in a single day"
    )
    max_total_loss = models.DecimalField(
        max_digits=12, 
        decimal_places=2,
        help_text="Maximum total drawdown from starting balance"
    )
    profit_target = models.DecimalField(
        max_digits=12, 
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Profit needed to pass evaluation"
    )
    min_trading_days = models.IntegerField(
        default=5,
        help_text="Minimum number of trading days required"
    )
    max_position_size = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal('100.00'),
        help_text="Maximum position size as percentage of balance"
    )
    
    # Payout Settings (for funded accounts)
    profit_split = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal('80.00'),
        help_text="Trader's percentage of profits"
    )
    
    # Status
    is_active = models.BooleanField(default=True)
    stripe_price_id = models.CharField(max_length=255, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['starting_balance']
        
    def __str__(self):
        return f"{self.name} - ${self.starting_balance}"


class PropFirmAccount(models.Model):
    """Individual trading account purchased by a user"""
    
    STATUS_CHOICES = [
        ('PENDING', 'Pending Payment'),
        ('ACTIVE', 'Active'),
        ('PASSED', 'Passed Evaluation'),
        ('FAILED', 'Failed'),
        ('SUSPENDED', 'Suspended'),
        ('CLOSED', 'Closed'),
    ]
    
    STAGE_CHOICES = [
        ('EVALUATION', 'Evaluation'),
        ('FUNDED', 'Funded'),
    ]
    
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='prop_accounts')
    plan = models.ForeignKey(PropFirmPlan, on_delete=models.PROTECT)
    
    # Account Identification
    account_number = models.CharField(max_length=20, unique=True, db_index=True)
    
    # Alpaca Account Details
    alpaca_account_id = models.CharField(max_length=100, blank=True)
    alpaca_account_status = models.CharField(max_length=50, blank=True)
    alpaca_buying_power = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    
    # Account Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    stage = models.CharField(max_length=20, choices=STAGE_CHOICES, default='EVALUATION')
    
    # Balance Tracking
    starting_balance = models.DecimalField(max_digits=12, decimal_places=2)
    current_balance = models.DecimalField(max_digits=12, decimal_places=2)
    high_water_mark = models.DecimalField(
        max_digits=12, 
        decimal_places=2,
        help_text="Highest balance reached"
    )
    
    # Rule Tracking
    daily_loss = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_loss = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    profit_earned = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_winning_trades = models.IntegerField(default=0)
    total_losing_trades = models.IntegerField(default=0)
    gross_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    gross_loss = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    profit_factor = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    trading_days = models.IntegerField(default=0)
    last_trade_date = models.DateField(null=True, blank=True)
    
    # Payment Information
    stripe_payment_intent_id = models.CharField(max_length=255, blank=True)
    stripe_subscription_id = models.CharField(max_length=255, blank=True, null=True)
    payment_completed_at = models.DateTimeField(null=True, blank=True)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    activated_at = models.DateTimeField(null=True, blank=True)
    passed_at = models.DateTimeField(null=True, blank=True)
    failed_at = models.DateTimeField(null=True, blank=True)
    closed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Notes
    failure_reason = models.TextField(blank=True)
    admin_notes = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'status']),
            models.Index(fields=['account_number']),
            models.Index(fields=['status', 'stage']),
        ]
        
    def __str__(self):
        return f"{self.account_number} - {self.user.email} - {self.status}"
    
    def generate_account_number(self):
        """Generate unique account number"""
        import random
        import string
        while True:
            number = 'PA' + ''.join(random.choices(string.digits, k=8))
            if not PropFirmAccount.objects.filter(account_number=number).exists():
                return number
    
    def save(self, *args, **kwargs):
        if not self.account_number:
            self.account_number = self.generate_account_number()
        if not self.starting_balance:
            self.starting_balance = self.plan.starting_balance
        if not self.current_balance:
            self.current_balance = self.starting_balance
        if not self.high_water_mark:
            self.high_water_mark = self.starting_balance
        super().save(*args, **kwargs)
    
    def activate(self):
        """Activate account after successful payment"""
        if self.status == 'PENDING':
            from core.services.alpaca_service import alpaca_service
            
            # Create paper trading account on Alpaca
            contact = {
                "email": self.user.email,
                "phone_number": getattr(self.user, 'phone', ''),
            }
            
            identity = {
                "given_name": self.user.first_name,
                "family_name": self.user.last_name,
            }
            
            try:
                # Create Alpaca paper account with plan's starting balance
                account = alpaca_service.create_paper_account(
                    nickname=f"PropFirm {self.account_number}",
                    initial_balance=self.starting_balance,
                    contact=contact,
                    identity=identity
                )
                
                # Store Alpaca account details
                self.alpaca_account_id = account.get('id')
                self.alpaca_account_status = account.get('status')
                self.alpaca_buying_power = Decimal(str(account.get('buying_power', '0')))
                
                self.status = 'ACTIVE'
                self.activated_at = timezone.now()
                self.save()
                
            except Exception as e:
                logger.error(f"Failed to create Alpaca account: {str(e)}")
                raise
    
    def check_rules(self):
        """Check if account has violated any rules"""
        from .services.rule_engine import RuleEngine
        engine = RuleEngine(self)
        return engine.check_all_rules()
    
    def calculate_daily_pnl(self):
        """Calculate P&L for today"""
        from paper_trading.models import PaperTrade
        today = timezone.now().date()
        
        trades_today = PaperTrade.objects.filter(
            user=self.user,
            status='CLOSED',
            exit_at__date=today
        )
        
        total_pl = sum(
            trade.realized_pl or Decimal('0') 
            for trade in trades_today
        )
        
        return total_pl
    
    def update_balance(self):
        """Recalculate current balance based on closed trades"""
        from paper_trading.models import PaperTrade
        from decimal import Decimal
        
        # Get all closed trades for this account
        closed_trades = PaperTrade.objects.filter(
            user=self.user,
            status='CLOSED',
            created_at__gte=self.created_at
        )
        
        # Reset counters
        self.total_winning_trades = 0
        self.total_losing_trades = 0
        self.gross_profit = Decimal('0')
        self.gross_loss = Decimal('0')
        
        total_realized_pl = Decimal('0')
        
        # Process each trade
        for trade in closed_trades:
            pl = trade.realized_pl or Decimal('0')
            total_realized_pl += pl
            
            if pl > 0:
                self.total_winning_trades += 1
                self.gross_profit += pl
            elif pl < 0:
                self.total_losing_trades += 1
                self.gross_loss += abs(pl)
        
        # Calculate profit factor
        self.profit_factor = (
            (self.gross_profit / self.gross_loss)
            if self.gross_loss > 0 and self.gross_profit > 0
            else Decimal('0')
        )
        
        self.current_balance = self.starting_balance + total_realized_pl
        self.profit_earned = max(Decimal('0'), total_realized_pl)
        
        # Update high water mark
        if self.current_balance > self.high_water_mark:
            self.high_water_mark = self.current_balance
        
        # Calculate losses
        if total_realized_pl < 0:
            self.total_loss = abs(total_realized_pl)
        
        self.save()
        
    def can_trade(self):
        """Check if account is allowed to trade"""
        return self.status == 'ACTIVE' and self.stage in ['EVALUATION', 'FUNDED']


class RuleViolation(models.Model):
    """Record of rule violations"""
    
    VIOLATION_TYPES = [
        ('DAILY_LOSS', 'Daily Loss Limit'),
        ('TOTAL_LOSS', 'Total Loss Limit'),
        ('POSITION_SIZE', 'Position Size Limit'),
        ('MIN_DAYS', 'Minimum Trading Days'),
    ]
    
    account = models.ForeignKey(PropFirmAccount, on_delete=models.CASCADE, related_name='violations')
    violation_type = models.CharField(max_length=50, choices=VIOLATION_TYPES)
    description = models.TextField()
    
    # Violation Details
    threshold_value = models.DecimalField(max_digits=12, decimal_places=2)
    actual_value = models.DecimalField(max_digits=12, decimal_places=2)
    
    # Related Trade (if applicable)
    related_trade = models.ForeignKey(
        'paper_trading.PaperTrade', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
        
    def __str__(self):
        return f"{self.account.account_number} - {self.violation_type}"


class Payout(models.Model):
    """Track payouts to traders for funded accounts"""
    
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('CANCELLED', 'Cancelled'),
    ]
    
    account = models.ForeignKey(PropFirmAccount, on_delete=models.CASCADE, related_name='payouts')
    
    # Payout Details
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    profit_earned = models.DecimalField(max_digits=12, decimal_places=2)
    profit_split = models.DecimalField(max_digits=5, decimal_places=2)
    
    # Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    
    # Payment Method
    payment_method = models.CharField(max_length=50, default='BANK_TRANSFER')
    payment_details = models.JSONField(default=dict, blank=True)
    
    # Stripe
    stripe_transfer_id = models.CharField(max_length=255, blank=True)
    
    # Timestamps
    requested_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    notes = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-requested_at']
        
    def __str__(self):
        return f"Payout {self.id} - {self.account.account_number} - ${self.amount}"
    
    def calculate_amount(self):
        """Calculate payout amount based on profit split"""
        self.amount = (self.profit_earned * self.profit_split) / Decimal('100')
        return self.amount


class AccountActivity(models.Model):
    """Audit log for account activities"""
    
    ACTIVITY_TYPES = [
        ('CREATED', 'Account Created'),
        ('ACTIVATED', 'Account Activated'),
        ('TRADE_PLACED', 'Trade Placed'),
        ('TRADE_CLOSED', 'Trade Closed'),
        ('RULE_VIOLATION', 'Rule Violation'),
        ('BALANCE_UPDATE', 'Balance Updated'),
        ('STATUS_CHANGE', 'Status Changed'),
        ('PAYOUT_REQUEST', 'Payout Requested'),
        ('NOTE_ADDED', 'Note Added'),
    ]
    
    account = models.ForeignKey(PropFirmAccount, on_delete=models.CASCADE, related_name='activities')
    activity_type = models.CharField(max_length=50, choices=ACTIVITY_TYPES)
    description = models.TextField()
    metadata = models.JSONField(default=dict, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, 
        on_delete=models.SET_NULL, 
        null=True,
        blank=True
    )
    
    class Meta:
        ordering = ['-created_at']
        verbose_name_plural = 'Account activities'
        
    def __str__(self):
        return f"{self.account.account_number} - {self.activity_type}"


class BillingDetail(models.Model):
    """Stored billing details for a user.

    NOTE: This model is intended for the mock payment flow used during
    development. Do NOT store raw card numbers or CVV in production.
    Replace this with a PCI-compliant integration (Stripe Customer / PaymentMethod)
    when moving to real payments.
    """

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='billing_details')
    cardholder_name = models.CharField(max_length=255)
    masked_number = models.CharField(max_length=32, help_text='Masked card number, e.g. **** **** **** 4242')
    last4 = models.CharField(max_length=4, blank=True)
    brand = models.CharField(max_length=50, blank=True)
    exp_month = models.CharField(max_length=2, blank=True)
    exp_year = models.CharField(max_length=4, blank=True)
    billing_address = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Billing for {self.user.email} - {self.masked_number}"