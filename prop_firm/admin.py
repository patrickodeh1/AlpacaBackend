from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import (
    PropFirmPlan, PropFirmAccount, RuleViolation,
    Payout, AccountActivity
)


@admin.register(PropFirmPlan)
class PropFirmPlanAdmin(admin.ModelAdmin):
    list_display = (
        'name', 'plan_type', 'starting_balance_display',
        'price_display', 'profit_target_display',
        'is_active', 'created_at'
    )
    list_filter = ('plan_type', 'is_active', 'created_at')
    search_fields = ('name', 'description')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'description', 'plan_type', 'is_active')
        }),
        ('Financial Terms', {
            'fields': ('starting_balance', 'price', 'profit_split')
        }),
        ('Trading Rules', {
            'fields': (
                'max_daily_loss', 'max_total_loss', 'profit_target',
                'min_trading_days', 'max_position_size'
            )
        }),
        ('Stripe Integration', {
            'fields': ('stripe_price_id',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def starting_balance_display(self, obj):
        return f"${obj.starting_balance:,.2f}"
    starting_balance_display.short_description = 'Starting Balance'
    
    def price_display(self, obj):
        return f"${obj.price:,.2f}"
    price_display.short_description = 'Price'
    
    def profit_target_display(self, obj):
        if obj.profit_target:
            return f"${obj.profit_target:,.2f}"
        return '-'
    profit_target_display.short_description = 'Profit Target'


class RuleViolationInline(admin.TabularInline):
    model = RuleViolation
    extra = 0
    readonly_fields = ('violation_type', 'description', 'threshold_value', 'actual_value', 'created_at')
    can_delete = False
    
    def has_add_permission(self, request, obj=None):
        return False


class AccountActivityInline(admin.TabularInline):
    model = AccountActivity
    extra = 0
    readonly_fields = ('activity_type', 'description', 'created_at', 'created_by')
    can_delete = False
    fields = ('activity_type', 'description', 'created_at')
    
    def has_add_permission(self, request, obj=None):
        return False


@admin.register(PropFirmAccount)
class PropFirmAccountAdmin(admin.ModelAdmin):
    list_display = (
        'account_number', 'user_email', 'plan_name',
        'status_badge', 'stage', 'balance_display',
        'pnl_display', 'trading_days', 'activated_at'
    )
    list_filter = (
        'status', 'stage', 'plan__plan_type',
        'created_at', 'activated_at'
    )
    search_fields = (
        'account_number', 'user__email', 'user__name',
        'plan__name'
    )
    readonly_fields = (
        'account_number', 'created_at', 'updated_at',
        'activated_at', 'passed_at', 'failed_at', 'closed_at',
        'stripe_payment_intent_id', 'payment_completed_at',
        'balance_info', 'rule_info'
    )
    
    fieldsets = (
        ('Account Information', {
            'fields': (
                'account_number', 'user', 'plan', 'status', 'stage'
            )
        }),
        ('Balance & Performance', {
            'fields': (
                'starting_balance', 'current_balance', 'high_water_mark',
                'profit_earned', 'total_loss', 'daily_loss',
                'balance_info'
            )
        }),
        ('Trading Activity', {
            'fields': (
                'trading_days', 'last_trade_date', 'rule_info'
            )
        }),
        ('Payment Information', {
            'fields': (
                'stripe_payment_intent_id', 'stripe_subscription_id',
                'payment_completed_at'
            ),
            'classes': ('collapse',)
        }),
        ('Status Tracking', {
            'fields': (
                'created_at', 'activated_at', 'passed_at',
                'failed_at', 'closed_at'
            )
        }),
        ('Notes', {
            'fields': ('failure_reason', 'admin_notes')
        })
    )
    
    inlines = [RuleViolationInline, AccountActivityInline]
    
    actions = ['activate_accounts', 'suspend_accounts', 'close_accounts', 'check_rules']
    
    def user_email(self, obj):
        return obj.user.email
    user_email.short_description = 'User Email'
    
    def plan_name(self, obj):
        return obj.plan.name
    plan_name.short_description = 'Plan'
    
    def status_badge(self, obj):
        colors = {
            'PENDING': 'orange',
            'ACTIVE': 'green',
            'PASSED': 'blue',
            'FAILED': 'red',
            'SUSPENDED': 'gray',
            'CLOSED': 'black',
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px;">{}</span>',
            color, obj.status
        )
    status_badge.short_description = 'Status'
    
    def balance_display(self, obj):
        return f"${obj.current_balance:,.2f}"
    balance_display.short_description = 'Balance'
    
    def pnl_display(self, obj):
        pnl = obj.current_balance - obj.starting_balance
        color = 'green' if pnl >= 0 else 'red'
        return format_html(
            '<span style="color: {};">${:,.2f}</span>',
            color, pnl
        )
    pnl_display.short_description = 'P&L'
    
    def balance_info(self, obj):
        pnl = obj.current_balance - obj.starting_balance
        pnl_pct = (pnl / obj.starting_balance * 100) if obj.starting_balance else 0
        
        html = f"""
        <table style="width: 100%;">
            <tr><td><b>Starting:</b></td><td>${obj.starting_balance:,.2f}</td></tr>
            <tr><td><b>Current:</b></td><td>${obj.current_balance:,.2f}</td></tr>
            <tr><td><b>High Water Mark:</b></td><td>${obj.high_water_mark:,.2f}</td></tr>
            <tr><td><b>P&L:</b></td><td style="color: {'green' if pnl >= 0 else 'red'};">
                ${pnl:,.2f} ({pnl_pct:+.2f}%)</td></tr>
            <tr><td><b>Profit:</b></td><td style="color: green;">${obj.profit_earned:,.2f}</td></tr>
            <tr><td><b>Total Loss:</b></td><td style="color: red;">${obj.total_loss:,.2f}</td></tr>
        </table>
        """
        return mark_safe(html)
    balance_info.short_description = 'Balance Overview'
    
    def rule_info(self, obj):
        plan = obj.plan
        
        # Calculate percentages
        daily_loss_pct = (obj.daily_loss / plan.max_daily_loss * 100) if plan.max_daily_loss else 0
        total_loss_pct = (obj.total_loss / plan.max_total_loss * 100) if plan.max_total_loss else 0
        profit_pct = (obj.profit_earned / plan.profit_target * 100) if plan.profit_target else 0
        
        html = f"""
        <table style="width: 100%;">
            <tr>
                <td><b>Daily Loss:</b></td>
                <td>${obj.daily_loss:,.2f} / ${plan.max_daily_loss:,.2f}</td>
                <td><progress value="{daily_loss_pct}" max="100"></progress> {daily_loss_pct:.1f}%</td>
            </tr>
            <tr>
                <td><b>Total Loss:</b></td>
                <td>${obj.total_loss:,.2f} / ${plan.max_total_loss:,.2f}</td>
                <td><progress value="{total_loss_pct}" max="100"></progress> {total_loss_pct:.1f}%</td>
            </tr>
            <tr>
                <td><b>Profit Target:</b></td>
                <td>${obj.profit_earned:,.2f} / ${plan.profit_target:,.2f}</td>
                <td><progress value="{profit_pct}" max="100"></progress> {profit_pct:.1f}%</td>
            </tr>
            <tr>
                <td><b>Trading Days:</b></td>
                <td>{obj.trading_days} / {plan.min_trading_days}</td>
                <td></td>
            </tr>
        </table>
        """
        return mark_safe(html)
    rule_info.short_description = 'Rule Progress'
    
    def activate_accounts(self, request, queryset):
        count = 0
        for account in queryset.filter(status='PENDING'):
            account.activate()
            count += 1
        self.message_user(request, f'{count} accounts activated.')
    activate_accounts.short_description = 'Activate selected accounts'
    
    def suspend_accounts(self, request, queryset):
        count = queryset.filter(status='ACTIVE').update(status='SUSPENDED')
        self.message_user(request, f'{count} accounts suspended.')
    suspend_accounts.short_description = 'Suspend selected accounts'
    
    def close_accounts(self, request, queryset):
        from django.utils import timezone
        count = 0
        for account in queryset.exclude(status='CLOSED'):
            account.status = 'CLOSED'
            account.closed_at = timezone.now()
            account.save()
            count += 1
        self.message_user(request, f'{count} accounts closed.')
    close_accounts.short_description = 'Close selected accounts'
    
    def check_rules(self, request, queryset):
        from .services.rule_engine import RuleEngine
        total_violations = 0
        for account in queryset.filter(status='ACTIVE'):
            account.update_balance()
            engine = RuleEngine(account)
            violations = engine.check_all_rules()
            total_violations += len(violations)
        
        self.message_user(
            request,
            f'Rule check completed. {total_violations} violations found.'
        )
    check_rules.short_description = 'Check rules for selected accounts'


@admin.register(RuleViolation)
class RuleViolationAdmin(admin.ModelAdmin):
    list_display = (
        'account_number', 'violation_type',
        'threshold_display', 'actual_display',
        'created_at'
    )
    list_filter = ('violation_type', 'created_at')
    search_fields = ('account__account_number', 'description')
    readonly_fields = ('account', 'violation_type', 'description', 'threshold_value', 'actual_value', 'related_trade', 'created_at')
    
    def account_number(self, obj):
        url = reverse('admin:prop_firm_propfirmaccount_change', args=[obj.account.id])
        return format_html('<a href="{}">{}</a>', url, obj.account.account_number)
    account_number.short_description = 'Account'
    
    def threshold_display(self, obj):
        return f"${obj.threshold_value:,.2f}"
    threshold_display.short_description = 'Threshold'
    
    def actual_display(self, obj):
        return format_html(
            '<span style="color: red;">${:,.2f}</span>',
            obj.actual_value
        )
    actual_display.short_description = 'Actual Value'
    
    def has_add_permission(self, request):
        return False


@admin.register(Payout)
class PayoutAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'account_number', 'amount_display',
        'status_badge', 'requested_at', 'completed_at'
    )
    list_filter = ('status', 'payment_method', 'requested_at')
    search_fields = ('account__account_number', 'account__user__email')
    readonly_fields = (
        'account', 'profit_earned', 'profit_split',
        'requested_at', 'processed_at', 'completed_at'
    )
    
    fieldsets = (
        ('Payout Information', {
            'fields': ('account', 'amount', 'profit_earned', 'profit_split', 'status')
        }),
        ('Payment Details', {
            'fields': ('payment_method', 'payment_details', 'stripe_transfer_id')
        }),
        ('Timestamps', {
            'fields': ('requested_at', 'processed_at', 'completed_at')
        }),
        ('Notes', {
            'fields': ('notes',)
        })
    )
    
    actions = ['approve_payouts', 'reject_payouts']
    
    def account_number(self, obj):
        return obj.account.account_number
    account_number.short_description = 'Account'
    
    def amount_display(self, obj):
        return f"${obj.amount:,.2f}"
    amount_display.short_description = 'Amount'
    
    def status_badge(self, obj):
        colors = {
            'PENDING': 'orange',
            'PROCESSING': 'blue',
            'COMPLETED': 'green',
            'FAILED': 'red',
            'CANCELLED': 'gray',
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px;">{}</span>',
            color, obj.status
        )
    status_badge.short_description = 'Status'
    
    def approve_payouts(self, request, queryset):
        from django.utils import timezone
        count = 0
        for payout in queryset.filter(status='PENDING'):
            payout.status = 'PROCESSING'
            payout.processed_at = timezone.now()
            payout.save()
            count += 1
        self.message_user(request, f'{count} payouts approved for processing.')
    approve_payouts.short_description = 'Approve selected payouts'
    
    def reject_payouts(self, request, queryset):
        count = queryset.filter(status='PENDING').update(status='CANCELLED')
        self.message_user(request, f'{count} payouts rejected.')
    reject_payouts.short_description = 'Reject selected payouts'


@admin.register(AccountActivity)
class AccountActivityAdmin(admin.ModelAdmin):
    list_display = (
        'account_number', 'activity_type',
        'description_short', 'created_at', 'created_by'
    )
    list_filter = ('activity_type', 'created_at')
    search_fields = ('account__account_number', 'description')
    readonly_fields = ('account', 'activity_type', 'description', 'metadata', 'created_at', 'created_by')
    
    def account_number(self, obj):
        return obj.account.account_number
    account_number.short_description = 'Account'
    
    def description_short(self, obj):
        return obj.description[:100] + '...' if len(obj.description) > 100 else obj.description
    description_short.short_description = 'Description'
    
    def has_add_permission(self, request):
        return False


# Customize admin site header
admin.site.site_header = "Prop Trading Firm Administration"
admin.site.site_title = "Prop Firm Admin"
admin.site.index_title = "Welcome to Prop Trading Firm Administration"