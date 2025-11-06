from rest_framework import serializers
from .models import PropFirmAccount, RuleViolation, PropFirmPlan
from django.contrib.auth import get_user_model

User = get_user_model()

class AdminAccountSerializer(serializers.ModelSerializer):
    """Admin serializer for prop firm accounts with user details"""
    user_email = serializers.EmailField(source='user.email', read_only=True)
    user_name = serializers.CharField(source='user.name', read_only=True)
    plan_name = serializers.CharField(source='plan.name', read_only=True)
    total_pnl = serializers.SerializerMethodField()
    pnl_percentage = serializers.SerializerMethodField()
    days_active = serializers.SerializerMethodField()
    
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
            'total_pnl', 'pnl_percentage', 'days_active',
            'failure_reason'
        ]
    
    def get_total_pnl(self, obj):
        return obj.current_balance - obj.starting_balance
    
    def get_pnl_percentage(self, obj):
        if obj.starting_balance == 0:
            return 0
        pnl = obj.current_balance - obj.starting_balance
        return (pnl / obj.starting_balance) * 100
    
    def get_days_active(self, obj):
        if not obj.activated_at:
            return 0
        from django.utils import timezone
        return (timezone.now() - obj.activated_at).days


class AdminRuleViolationSerializer(serializers.ModelSerializer):
    """Admin serializer for rule violations with account details"""
    account_number = serializers.CharField(source='account.account_number', read_only=True)
    user_email = serializers.EmailField(source='account.user.email', read_only=True)
    
    class Meta:
        model = RuleViolation
        fields = [
            'id', 'account', 'account_number', 'user_email',
            'violation_type', 'description',
            'threshold_value', 'actual_value',
            'related_trade', 'created_at'
        ]