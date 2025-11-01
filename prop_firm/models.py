from decimal import Decimal
from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone
from django.db.models import Sum


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
            self.status = 'ACTIVE'
            self.activated_at = timezone.now()
            self.save()
    
    def check_rules(self):
        """Check if account has violated any rules"""
        from .services.rule_engine import RuleEngine
        engine = RuleEngine(self)
        return engine.check_all_rules()
    
    def calculate_daily_pnl(self):
        """Calculate P&L for today"""
        from apps.paper_trading.models import PaperTrade
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
        from apps.paper_trading.models import PaperTrade
        
        # Get all closed trades for this account
        closed_trades = PaperTrade.objects.filter(
            user=self.user,
            status='CLOSED',
            created_at__gte=self.created_at
        )
        
        total_realized_pl = sum(
            trade.realized_pl or Decimal('0') 
            for trade in closed_trades
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