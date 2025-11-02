from rest_framework import serializers
from decimal import Decimal
from .models import (
    PropFirmPlan, PropFirmAccount, RuleViolation,
    Payout, AccountActivity
)


class PropFirmPlanSerializer(serializers.ModelSerializer):
    """Serializer for prop firm plans"""
    
    class Meta:
        model = PropFirmPlan
        fields = [
            'id', 'name', 'description', 'plan_type',
            'starting_balance', 'price',
            'max_daily_loss', 'max_total_loss', 'profit_target',
            'min_trading_days', 'max_position_size', 'profit_split',
            'is_active', 'created_at'
        ]
        read_only_fields = ['created_at']


class RuleViolationSerializer(serializers.ModelSerializer):
    """Serializer for rule violations"""
    
    class Meta:
        model = RuleViolation
        fields = [
            'id', 'violation_type', 'description',
            'threshold_value', 'actual_value',
            'related_trade', 'created_at'
        ]
        read_only_fields = ['created_at']


class AccountActivitySerializer(serializers.ModelSerializer):
    """Serializer for account activities"""
    
    created_by_name = serializers.CharField(source='created_by.name', read_only=True)
    
    class Meta:
        model = AccountActivity
        fields = [
            'id', 'activity_type', 'description',
            'metadata', 'created_at', 'created_by', 'created_by_name'
        ]
        read_only_fields = ['created_at']


class PropFirmAccountSerializer(serializers.ModelSerializer):
    """Serializer for prop firm accounts"""
    
    plan_name = serializers.CharField(source='plan.name', read_only=True)
    plan_details = PropFirmPlanSerializer(source='plan', read_only=True)
    violations = RuleViolationSerializer(many=True, read_only=True)
    recent_activities = serializers.SerializerMethodField()
    
    # Calculated fields
    total_pnl = serializers.SerializerMethodField()
    pnl_percentage = serializers.SerializerMethodField()
    days_active = serializers.SerializerMethodField()
    can_trade = serializers.SerializerMethodField()
    
    class Meta:
        model = PropFirmAccount
        fields = [
            'id', 'account_number', 'status', 'stage',
            'plan', 'plan_name', 'plan_details',
            'starting_balance', 'current_balance', 'high_water_mark',
            'daily_loss', 'total_loss', 'profit_earned',
            'trading_days', 'last_trade_date',
            'created_at', 'activated_at', 'passed_at', 'failed_at', 'closed_at',
            'failure_reason', 'violations', 'recent_activities',
            'total_pnl', 'pnl_percentage', 'days_active', 'can_trade'
        ]
        read_only_fields = [
            'account_number', 'status', 'stage',
            'starting_balance', 'current_balance', 'high_water_mark',
            'daily_loss', 'total_loss', 'profit_earned',
            'trading_days', 'last_trade_date',
            'created_at', 'activated_at', 'passed_at', 'failed_at', 'closed_at',
            'failure_reason'
        ]
    
    def get_total_pnl(self, obj):
        """Calculate total P&L"""
        return obj.current_balance - obj.starting_balance
    
    def get_pnl_percentage(self, obj):
        """Calculate P&L percentage"""
        if obj.starting_balance == 0:
            return Decimal('0')
        pnl = obj.current_balance - obj.starting_balance
        return (pnl / obj.starting_balance) * Decimal('100')
    
    def get_days_active(self, obj):
        """Calculate days since activation"""
        if not obj.activated_at:
            return 0
        from django.utils import timezone
        delta = timezone.now() - obj.activated_at
        return delta.days
    
    def get_can_trade(self, obj):
        """Check if account can trade"""
        return obj.can_trade()
    
    def get_recent_activities(self, obj):
        """Get recent activities"""
        activities = obj.activities.all()[:5]
        return AccountActivitySerializer(activities, many=True).data


class PropFirmAccountListSerializer(serializers.ModelSerializer):
    """Simplified serializer for account list"""
    
    plan_name = serializers.CharField(source='plan.name', read_only=True)
    total_pnl = serializers.SerializerMethodField()
    pnl_percentage = serializers.SerializerMethodField()
    
    class Meta:
        model = PropFirmAccount
        fields = [
            'id', 'account_number', 'status', 'stage',
            'plan_name', 'starting_balance', 'current_balance',
            'profit_earned', 'trading_days',
            'created_at', 'activated_at',
            'total_pnl', 'pnl_percentage'
        ]
    
    def get_total_pnl(self, obj):
        return obj.current_balance - obj.starting_balance
    
    def get_pnl_percentage(self, obj):
        if obj.starting_balance == 0:
            return Decimal('0')
        pnl = obj.current_balance - obj.starting_balance
        return (pnl / obj.starting_balance) * Decimal('100')


class PayoutSerializer(serializers.ModelSerializer):
    """Serializer for payouts"""
    
    account_number = serializers.CharField(source='account.account_number', read_only=True)
    
    class Meta:
        model = Payout
        fields = [
            'id', 'account', 'account_number',
            'amount', 'profit_earned', 'profit_split',
            'status', 'payment_method', 'payment_details',
            'requested_at', 'processed_at', 'completed_at',
            'notes'
        ]
        read_only_fields = [
            'amount', 'requested_at', 'processed_at', 'completed_at'
        ]


class PayoutRequestSerializer(serializers.Serializer):
    """Serializer for payout requests"""
    
    account_id = serializers.IntegerField()
    payment_method = serializers.CharField(max_length=50)
    payment_details = serializers.JSONField(required=False)
    
    def validate_account_id(self, value):
        """Validate account exists and is eligible for payout"""
        try:
            account = PropFirmAccount.objects.get(id=value)
        except PropFirmAccount.DoesNotExist:
            raise serializers.ValidationError("Account not found")
        
        if account.stage != 'FUNDED':
            raise serializers.ValidationError("Only funded accounts can request payouts")
        
        if account.status != 'ACTIVE':
            raise serializers.ValidationError("Account must be active to request payout")
        
        if account.profit_earned <= 0:
            raise serializers.ValidationError("No profits available for payout")
        
        return value


class CheckoutSessionSerializer(serializers.Serializer):
    """Serializer for creating checkout sessions"""
    
    plan_id = serializers.IntegerField()
    success_url = serializers.URLField()
    cancel_url = serializers.URLField()
    
    def validate_plan_id(self, value):
        """Validate plan exists and is active"""
        try:
            plan = PropFirmPlan.objects.get(id=value, is_active=True)
        except PropFirmPlan.DoesNotExist:
            raise serializers.ValidationError("Plan not found or inactive")
        return value


class WebhookEventSerializer(serializers.Serializer):
    """Serializer for Stripe webhook events"""
    
    type = serializers.CharField()
    data = serializers.JSONField()