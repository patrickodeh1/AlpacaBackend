from decimal import Decimal
from django.utils import timezone
from django.db.models import Sum
import logging

logger = logging.getLogger(__name__)


class RuleEngine:
    """Enforce trading rules on prop firm accounts"""
    
    def __init__(self, account):
        self.account = account
        self.plan = account.plan
        self.violations = []
        
    def check_all_rules(self):
        """Check all rules and return violations"""
        self.violations = []
        
        if self.account.status not in ['ACTIVE', 'PASSED']:
            return self.violations
        
        # Check each rule
        self.check_daily_loss_limit()
        self.check_total_loss_limit()
        self.check_profit_target()
        self.check_minimum_trading_days()
        
        # If violations found, handle them
        if self.violations:
            self._handle_violations()
            
        return self.violations
    
    def check_daily_loss_limit(self):
        """Check if daily loss limit has been exceeded"""
        from paper_trading.models import PaperTrade
        
        today = timezone.now().date()
        
        # Get today's trades
        trades_today = PaperTrade.objects.filter(
            user=self.account.user,
            status='CLOSED',
            exit_at__date=today,
            created_at__gte=self.account.created_at
        )
        
        daily_pnl = sum(
            trade.realized_pl or Decimal('0') 
            for trade in trades_today
        )
        
        # Update daily loss tracking
        if daily_pnl < 0:
            self.account.daily_loss = abs(daily_pnl)
            self.account.save()
        
        # Check if limit exceeded
        if abs(daily_pnl) > self.plan.max_daily_loss:
            violation = {
                'type': 'DAILY_LOSS',
                'description': f'Daily loss limit exceeded: ${abs(daily_pnl):.2f} > ${self.plan.max_daily_loss:.2f}',
                'threshold': self.plan.max_daily_loss,
                'actual': abs(daily_pnl)
            }
            self.violations.append(violation)
            logger.warning(f"Daily loss violation for account {self.account.account_number}")
        
        return daily_pnl
    
    def check_total_loss_limit(self):
        """Check if total loss limit has been exceeded"""
        # Calculate total loss from starting balance
        drawdown = self.account.starting_balance - self.account.current_balance
        
        if drawdown < 0:
            drawdown = Decimal('0')
        
        if drawdown > self.plan.max_total_loss:
            violation = {
                'type': 'TOTAL_LOSS',
                'description': f'Total loss limit exceeded: ${drawdown:.2f} > ${self.plan.max_total_loss:.2f}',
                'threshold': self.plan.max_total_loss,
                'actual': drawdown
            }
            self.violations.append(violation)
            logger.warning(f"Total loss violation for account {self.account.account_number}")
        
        return drawdown
    
    def check_profit_target(self):
        """Check if profit target has been reached (for evaluation accounts)"""
        if self.account.stage != 'EVALUATION' or not self.plan.profit_target:
            return None
        
        if self.account.profit_earned >= self.plan.profit_target:
            # Check if minimum trading days met
            if self.account.trading_days >= self.plan.min_trading_days:
                logger.info(f"Account {self.account.account_number} passed evaluation!")
                self._pass_evaluation()
            else:
                logger.info(f"Account {self.account.account_number} hit profit target but needs more trading days")
        
        return self.account.profit_earned
    
    def check_minimum_trading_days(self):
        """Update trading days count"""
        from paper_trading.models import PaperTrade
        
        # Get unique trading dates
        trading_dates = PaperTrade.objects.filter(
            user=self.account.user,
            status='CLOSED',
            created_at__gte=self.account.created_at
        ).dates('exit_at', 'day')
        
        self.account.trading_days = len(list(trading_dates))
        
        # Update last trade date
        if trading_dates:
            self.account.last_trade_date = max(trading_dates)
        
        self.account.save()
        
        return self.account.trading_days
    
    def check_position_size(self, trade_quantity, trade_price):
        """Check if position size exceeds limit"""
        position_value = trade_quantity * trade_price
        max_position_value = (self.account.current_balance * self.plan.max_position_size) / Decimal('100')
        
        if position_value > max_position_value:
            violation = {
                'type': 'POSITION_SIZE',
                'description': f'Position size exceeds limit: ${position_value:.2f} > ${max_position_value:.2f}',
                'threshold': max_position_value,
                'actual': position_value
            }
            return violation
        
        return None
    
    def _handle_violations(self):
        """Handle rule violations - fail account and record violations"""
        from prop_firm.models import RuleViolation, AccountActivity
        
        # Fail the account
        self.account.status = 'FAILED'
        self.account.failed_at = timezone.now()
        
        # Build failure reason
        failure_reasons = [v['description'] for v in self.violations]
        self.account.failure_reason = '\n'.join(failure_reasons)
        self.account.save()
        
        # Record violations
        for violation in self.violations:
            RuleViolation.objects.create(
                account=self.account,
                violation_type=violation['type'],
                description=violation['description'],
                threshold_value=violation['threshold'],
                actual_value=violation['actual']
            )
        
        # Log activity
        AccountActivity.objects.create(
            account=self.account,
            activity_type='RULE_VIOLATION',
            description=f"Account failed due to rule violations: {', '.join([v['type'] for v in self.violations])}",
            metadata={'violations': self.violations}
        )
        
        logger.error(f"Account {self.account.account_number} failed due to violations")
    
    def _pass_evaluation(self):
        """Mark account as passed evaluation"""
        from prop_firm.models import AccountActivity
        
        self.account.status = 'PASSED'
        self.account.passed_at = timezone.now()
        self.account.save()
        
        # Log activity
        AccountActivity.objects.create(
            account=self.account,
            activity_type='STATUS_CHANGE',
            description=f"Account passed evaluation with ${self.account.profit_earned:.2f} profit in {self.account.trading_days} trading days",
            metadata={
                'profit': str(self.account.profit_earned),
                'trading_days': self.account.trading_days
            }
        )
        
        # TODO: Send notification to user
        logger.info(f"Account {self.account.account_number} passed evaluation")


class TradeValidator:
    """Validate trades before they're placed"""
    
    def __init__(self, account):
        self.account = account
        self.rule_engine = RuleEngine(account)
        
    def can_place_trade(self, asset, direction, quantity, price):
        """Check if trade can be placed"""
        errors = []
        
        # Check account status
        if not self.account.can_trade():
            errors.append(f"Account is not active for trading (status: {self.account.status})")
            return False, errors
        
        # Check position size
        violation = self.rule_engine.check_position_size(quantity, price)
        if violation:
            errors.append(violation['description'])
        
        # Check if account has open positions that would exceed limits
        from paper_trading.models import PaperTrade
        
        open_positions_value = PaperTrade.objects.filter(
            user=self.account.user,
            status='OPEN',
            created_at__gte=self.account.created_at
        ).aggregate(
            total=Sum('entry_price')
        )['total'] or Decimal('0')
        
        new_position_value = quantity * price
        total_exposure = open_positions_value + new_position_value
        
        max_total_exposure = self.account.current_balance * Decimal('2.0')  # 200% leverage max
        
        if total_exposure > max_total_exposure:
            errors.append(f"Total exposure would exceed limit: ${total_exposure:.2f} > ${max_total_exposure:.2f}")
        
        return len(errors) == 0, errors