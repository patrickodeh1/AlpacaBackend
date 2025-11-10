# prop_firm/services/rule_engine.py
"""
Rule Engine for Prop Firm Trading Rules Validation
"""

import logging
from decimal import Decimal
from typing import List, Tuple
from datetime import date

from django.utils import timezone
from django.db.models import Sum

from prop_firm.models import PropFirmAccount, RuleViolation
from core.models import Asset
from paper_trading.models import PaperTrade

logger = logging.getLogger(__name__)


class TradeValidator:
    """
    Validates if a trade can be placed according to prop firm rules.
    This is SIMULATION-ONLY and doesn't interact with Alpaca broker API.
    """
    
    def __init__(self, account: PropFirmAccount):
        self.account = account
        self.plan = account.plan
    
    def can_place_trade(
        self,
        asset: Asset,
        direction: str,
        quantity: Decimal,
        price: Decimal
    ) -> Tuple[bool, List[str]]:
        """
        Check if a trade can be placed according to all rules.
        
        Returns:
            (can_trade: bool, errors: List[str])
        """
        errors = []
        
        # 1. Check account status
        if not self.account.can_trade():
            errors.append(f"Account status {self.account.status} cannot place trades")
            return False, errors
        
        # 2. Check position size
        position_value = quantity * price
        max_position_value = (
            self.account.current_balance * self.plan.max_position_size / Decimal('100')
        )
        
        if position_value > max_position_value:
            errors.append(
                f"Position size ${position_value:.2f} exceeds maximum "
                f"${max_position_value:.2f} ({self.plan.max_position_size}% of balance)"
            )
        
        # 3. Check daily loss limit (if we've already lost today)
        if self.account.daily_loss > self.plan.max_daily_loss:
            errors.append(
                f"Daily loss limit reached: ${self.account.daily_loss:.2f} / "
                f"${self.plan.max_daily_loss:.2f}"
            )
        
        # 4. Check total loss limit
        if self.account.total_loss > self.plan.max_total_loss:
            errors.append(
                f"Total loss limit reached: ${self.account.total_loss:.2f} / "
                f"${self.plan.max_total_loss:.2f}"
            )
        
        # 5. Check if we have enough balance (margin check for simulation)
        required_margin = position_value  # 1:1 for safety in simulation
        if required_margin > self.account.current_balance:
            errors.append(
                f"Insufficient balance: need ${required_margin:.2f}, "
                f"have ${self.account.current_balance:.2f}"
            )
        
        # 6. REMOVED: No alpaca_account_id check needed for simulation
        # This is purely simulated trading using market data
        
        if errors:
            logger.warning(
                f"Trade validation failed for {self.account.account_number}: "
                f"{'; '.join(errors)}"
            )
            return False, errors
        
        return True, []
    
    def validate_position_size(self, quantity: Decimal, price: Decimal) -> bool:
        """Check if position size is within limits"""
        position_value = quantity * price
        max_position_value = (
            self.account.current_balance * self.plan.max_position_size / Decimal('100')
        )
        return position_value <= max_position_value


class RuleEngine:
    """
    Checks and enforces prop firm trading rules.
    Records violations and updates account status.
    """
    
    def __init__(self, account: PropFirmAccount):
        self.account = account
        self.plan = account.plan
    
    def check_all_rules(self) -> List[dict]:
        """
        Check all rules and record violations.
        
        Returns:
            List of violation dictionaries
        """
        violations = []
        
        # Check daily loss
        daily_violation = self.check_daily_loss()
        if daily_violation:
            violations.append(daily_violation)
        
        # Check total loss
        total_violation = self.check_total_loss()
        if total_violation:
            violations.append(total_violation)
        
        # Check minimum trading days (for evaluation accounts)
        if self.account.stage == 'EVALUATION':
            days_violation = self.check_minimum_trading_days()
            if days_violation:
                violations.append(days_violation)
        
        # If critical violations, fail the account
        if violations:
            self._handle_violations(violations)
        
        return violations
    
    def check_daily_loss(self) -> dict | None:
        """Check if daily loss limit is exceeded"""
        if self.account.daily_loss > self.plan.max_daily_loss:
            violation = {
                'type': 'DAILY_LOSS',
                'description': f'Daily loss limit exceeded: ${self.account.daily_loss:.2f}',
                'threshold': float(self.plan.max_daily_loss),
                'actual': float(self.account.daily_loss)
            }
            
            # Record in database
            RuleViolation.objects.create(
                account=self.account,
                violation_type='DAILY_LOSS',
                description=violation['description'],
                threshold_value=self.plan.max_daily_loss,
                actual_value=self.account.daily_loss
            )
            
            logger.warning(
                f"Daily loss violation: {self.account.account_number} - "
                f"${self.account.daily_loss:.2f} > ${self.plan.max_daily_loss:.2f}"
            )
            
            return violation
        
        return None
    
    def check_total_loss(self) -> dict | None:
        """Check if total loss limit is exceeded"""
        if self.account.total_loss > self.plan.max_total_loss:
            violation = {
                'type': 'TOTAL_LOSS',
                'description': f'Total loss limit exceeded: ${self.account.total_loss:.2f}',
                'threshold': float(self.plan.max_total_loss),
                'actual': float(self.account.total_loss)
            }
            
            # Record in database
            RuleViolation.objects.create(
                account=self.account,
                violation_type='TOTAL_LOSS',
                description=violation['description'],
                threshold_value=self.plan.max_total_loss,
                actual_value=self.account.total_loss
            )
            
            logger.warning(
                f"Total loss violation: {self.account.account_number} - "
                f"${self.account.total_loss:.2f} > ${self.plan.max_total_loss:.2f}"
            )
            
            return violation
        
        return None
    
    def check_minimum_trading_days(self) -> dict | None:
        """Check if minimum trading days requirement is met for evaluation"""
        if self.account.trading_days < self.plan.min_trading_days:
            # This is not a violation yet, just a requirement check
            return None
        
        return None
    
    def check_profit_target(self) -> bool:
        """Check if profit target is met (for passing evaluation)"""
        if self.plan.profit_target is None:
            return False
        
        return self.account.profit_earned >= self.plan.profit_target
    
    def check_evaluation_pass(self) -> bool:
        """
        Check if account has passed evaluation stage.
        
        Requirements:
        1. Minimum trading days met
        2. Profit target met
        3. No active violations
        4. Account status is ACTIVE
        """
        if self.account.stage != 'EVALUATION':
            return False
        
        if self.account.status != 'ACTIVE':
            return False
        
        # Check trading days
        if self.account.trading_days < self.plan.min_trading_days:
            logger.info(
                f"Account {self.account.account_number} needs more trading days: "
                f"{self.account.trading_days}/{self.plan.min_trading_days}"
            )
            return False
        
        # Check profit target
        if not self.check_profit_target():
            logger.info(
                f"Account {self.account.account_number} has not met profit target: "
                f"${self.account.profit_earned:.2f}/${self.plan.profit_target:.2f}"
            )
            return False
        
        # Check for active violations
        has_violations = RuleViolation.objects.filter(
            account=self.account
        ).exists()
        
        if has_violations:
            logger.info(
                f"Account {self.account.account_number} has violations, cannot pass"
            )
            return False
        
        logger.info(f"Account {self.account.account_number} has passed evaluation!")
        return True
    
    def _handle_violations(self, violations: List[dict]):
        """Handle rule violations by updating account status"""
        # Check for critical violations that should fail the account
        critical_types = ['DAILY_LOSS', 'TOTAL_LOSS']
        
        has_critical = any(v['type'] in critical_types for v in violations)
        
        if has_critical and self.account.status == 'ACTIVE':
            self.account.status = 'FAILED'
            self.account.failed_at = timezone.now()
            
            violation_summary = '; '.join([v['description'] for v in violations])
            self.account.failure_reason = violation_summary
            
            self.account.save()
            
            logger.error(
                f"Account {self.account.account_number} FAILED due to violations: "
                f"{violation_summary}"
            )
    
    def update_daily_stats(self):
        """
        Update daily statistics and reset daily loss if new day.
        Should be called periodically or when closing trades.
        """
        today = date.today()
        
        # Check if it's a new trading day
        if self.account.last_trade_date and self.account.last_trade_date < today:
            # Reset daily loss for new day
            self.account.daily_loss = Decimal('0')
            logger.info(f"Reset daily loss for {self.account.account_number}")
        
        # Calculate today's closed trades P&L
        today_trades = PaperTrade.objects.filter(
            user=self.account.user,
            status='CLOSED',
            exit_at__date=today
        )
        
        daily_pl = sum(
            Decimal(str(trade.realized_pl or 0))
            for trade in today_trades
        )
        
        if daily_pl < 0:
            self.account.daily_loss = abs(daily_pl)
        else:
            self.account.daily_loss = Decimal('0')
        
        self.account.save()


def check_and_update_account_rules(account: PropFirmAccount):
    """
    Convenience function to check all rules and update account.
    Call this after closing trades or periodically.
    """
    engine = RuleEngine(account)
    
    # Update daily stats
    engine.update_daily_stats()
    
    # Check all rules
    violations = engine.check_all_rules()
    
    # Check if evaluation passed
    if engine.check_evaluation_pass():
        account.status = 'PASSED'
        account.passed_at = timezone.now()
        account.stage = 'FUNDED'
        account.save()
        logger.info(f"Account {account.account_number} promoted to FUNDED stage!")
    
    return violations