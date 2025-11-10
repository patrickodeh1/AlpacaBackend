from rest_framework import serializers
from .models import PropFirmAccount, RuleViolation, PropFirmPlan, Payout, AccountActivity
from paper_trading.models import PaperTrade
from core.models import Asset, WatchList, AlpacaAccount
from django.contrib.auth import get_user_model
from django.utils import timezone

User = get_user_model()


class AdminDashboardSerializer(serializers.Serializer):
    """Serializer for admin dashboard overview data"""
    users_count = serializers.IntegerField()
    accounts_count = serializers.IntegerField()
    plans_count = serializers.IntegerField()
    payouts_count = serializers.IntegerField()
    active_accounts = serializers.IntegerField()
    total_balance = serializers.DecimalField(max_digits=12, decimal_places=2)
    recent_accounts = serializers.ListField()
    recent_violations = serializers.ListField()
    revenue_stats = serializers.DictField()


class AdminAccountSerializer(serializers.ModelSerializer):
    """Admin serializer for prop firm accounts with user details"""
    user_email = serializers.EmailField(source='user.email', read_only=True)
    user_name = serializers.CharField(source='user.name', read_only=True)
    plan_name = serializers.CharField(source='plan.name', read_only=True)
    total_pnl = serializers.SerializerMethodField()
    pnl_percentage = serializers.SerializerMethodField()
    days_active = serializers.SerializerMethodField()
    open_trades = serializers.SerializerMethodField()
    
    class Meta:
        model = PropFirmAccount
        fields = [
            'id', 'account_number', 'status', 'stage',
            'user_email', 'user_name', 'plan_name',
            'starting_balance', 'current_balance', 'high_water_mark',
            'daily_loss', 'total_loss', 'profit_earned',
            'total_winning_trades', 'total_losing_trades',
            'gross_profit', 'gross_loss', 'profit_factor',
            'trading_days', 'last_trade_date',
            'created_at', 'activated_at', 'passed_at', 'failed_at',
            'total_pnl', 'pnl_percentage', 'days_active', 'open_trades',
            'failure_reason', 'admin_notes'
        ]
    
    def get_total_pnl(self, obj):
        return obj.current_balance - obj.starting_balance
    
    def get_pnl_percentage(self, obj):
        if obj.starting_balance == 0:
            return 0
        pnl = obj.current_balance - obj.starting_balance
        return float((pnl / obj.starting_balance) * 100)
    
    def get_days_active(self, obj):
        if not obj.activated_at:
            return 0
        return (timezone.now() - obj.activated_at).days
    
    def get_open_trades(self, obj):
        return PaperTrade.objects.filter(
            user=obj.user,
            status='OPEN'
        ).count()


class AdminRuleViolationSerializer(serializers.ModelSerializer):
    """Admin serializer for rule violations with account details"""
    account_number = serializers.CharField(source='account.account_number', read_only=True)
    user_email = serializers.EmailField(source='account.user.email', read_only=True)
    trade_symbol = serializers.SerializerMethodField()
    
    class Meta:
        model = RuleViolation
        fields = [
            'id', 'account', 'account_number', 'user_email',
            'violation_type', 'description',
            'threshold_value', 'actual_value',
            'related_trade', 'trade_symbol', 'created_at'
        ]
    
    def get_trade_symbol(self, obj):
        if obj.related_trade:
            return obj.related_trade.asset.symbol
        return None


class AdminPlanSerializer(serializers.ModelSerializer):
    """Admin serializer for prop firm plans"""
    active_accounts = serializers.SerializerMethodField()
    total_revenue = serializers.SerializerMethodField()
    
    class Meta:
        model = PropFirmPlan
        fields = [
            'id', 'name', 'description', 'plan_type',
            'starting_balance', 'price',
            'max_daily_loss', 'max_total_loss', 'profit_target',
            'min_trading_days', 'max_position_size', 'profit_split',
            'is_active', 'stripe_price_id',
            'created_at', 'updated_at',
            'active_accounts', 'total_revenue'
        ]
    
    def get_active_accounts(self, obj):
        return PropFirmAccount.objects.filter(
            plan=obj,
            status='ACTIVE'
        ).count()
    
    def get_total_revenue(self, obj):
        from django.db.models import Sum
        revenue = PropFirmAccount.objects.filter(
            plan=obj,
            payment_completed_at__isnull=False
        ).aggregate(total=Sum('plan__price'))['total']
        return revenue or 0


class AdminPayoutSerializer(serializers.ModelSerializer):
    """Admin serializer for payouts"""
    account_number = serializers.CharField(source='account.account_number', read_only=True)
    user_email = serializers.EmailField(source='account.user.email', read_only=True)
    user_name = serializers.CharField(source='account.user.name', read_only=True)
    
    class Meta:
        model = Payout
        fields = [
            'id', 'account', 'account_number', 'user_email', 'user_name',
            'amount', 'profit_earned', 'profit_split',
            'status', 'payment_method', 'payment_details',
            'stripe_transfer_id',
            'requested_at', 'processed_at', 'completed_at',
            'notes'
        ]


class AdminUserSerializer(serializers.ModelSerializer):
    """Admin serializer for users"""
    accounts_count = serializers.SerializerMethodField()
    total_balance = serializers.SerializerMethodField()
    active_trades = serializers.SerializerMethodField()
    
    class Meta:
        model = User
        fields = [
            'id', 'email', 'name', 'is_admin', 'is_verified',
            'auth_provider', 'created_at', 'last_login',
            'accounts_count', 'total_balance', 'active_trades'
        ]
    
    def get_accounts_count(self, obj):
        return PropFirmAccount.objects.filter(user=obj).count()
    
    def get_total_balance(self, obj):
        from django.db.models import Sum
        total = PropFirmAccount.objects.filter(
            user=obj
        ).aggregate(total=Sum('current_balance'))['total']
        return total or 0
    
    def get_active_trades(self, obj):
        return PaperTrade.objects.filter(
            user=obj,
            status='OPEN'
        ).count()


class AdminAssetSerializer(serializers.ModelSerializer):
    """Admin serializer for assets"""
    watchlists_count = serializers.SerializerMethodField()
    trades_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Asset
        fields = [
            'id', 'alpaca_id', 'symbol', 'name',
            'asset_class', 'exchange', 'status',
            'tradable', 'marginable', 'shortable',
            'easy_to_borrow', 'fractionable',
            'created_at', 'updated_at',
            'watchlists_count', 'trades_count'
        ]
    
    def get_watchlists_count(self, obj):
        from core.models import WatchListAsset
        return WatchListAsset.objects.filter(asset=obj).count()
    
    def get_trades_count(self, obj):
        return PaperTrade.objects.filter(asset=obj).count()


class AdminWatchlistSerializer(serializers.ModelSerializer):
    """Admin serializer for watchlists"""
    user_email = serializers.EmailField(source='user.email', read_only=True)
    assets_count = serializers.SerializerMethodField()
    
    class Meta:
        model = WatchList
        fields = [
            'id', 'user', 'user_email', 'name', 'description',
            'is_active', 'is_default',
            'created_at', 'updated_at',
            'assets_count'
        ]
    
    def get_assets_count(self, obj):
        from core.models import WatchListAsset
        return WatchListAsset.objects.filter(watchlist=obj).count()