# prop_firm/services/trade_simulation.py
"""
Trade Simulation Engine

Simulates trade execution using real-time Alpaca market data without
actually placing trades through Alpaca API. This allows unlimited users
without needing Broker API access.
"""

import logging
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional, Tuple

from django.utils import timezone
from django.db import transaction

from core.services.alpaca_service import alpaca_service
from core.models import Asset, Candle
from paper_trading.models import PaperTrade
from prop_firm.models import PropFirmAccount

logger = logging.getLogger(__name__)


class TradeSimulationEngine:
    """
    Simulates realistic trade execution using Alpaca market data.
    
    Features:
    - Real-time price fetching from Alpaca
    - Simulated slippage for market orders
    - Instant execution for better UX
    - Proper order state management
    """
    
    # Simulation parameters (configurable)
    SLIPPAGE_PERCENTAGE = Decimal('0.0005')  # 0.05% slippage
    COMMISSION_PER_SHARE = Decimal('0.00')   # Commission-free for now
    MIN_COMMISSION = Decimal('0.00')
    
    def __init__(self, account: PropFirmAccount):
        self.account = account
        
    def get_current_price(self, asset: Asset) -> Optional[Decimal]:
        """
        Get current market price for an asset from Alpaca.
        
        Uses multiple fallback strategies:
        1. Latest tick from database (if recent)
        2. Latest 1-minute candle close
        3. Alpaca real-time quote API
        """
        # Strategy 1: Recent tick from database (last 5 minutes)
        from core.models import Tick
        recent_tick = Tick.objects.filter(
            asset=asset,
            timestamp__gte=timezone.now() - timedelta(minutes=5)
        ).order_by('-timestamp').first()
        
        if recent_tick:
            logger.debug(f"Using recent tick price for {asset.symbol}: ${recent_tick.price}")
            return Decimal(str(recent_tick.price))
        
        # Strategy 2: Latest 1-minute candle
        latest_candle = Candle.objects.filter(
            asset=asset,
            timeframe='1T'
        ).order_by('-timestamp').first()
        
        if latest_candle:
            # Use close price from most recent candle
            logger.debug(f"Using latest candle close for {asset.symbol}: ${latest_candle.close}")
            return Decimal(str(latest_candle.close))
        
        # Strategy 3: Fetch from Alpaca API (fallback)
        try:
            # Get latest bar from Alpaca
            from datetime import datetime
            end = datetime.now()
            start = end - timedelta(days=1)
            
            response = alpaca_service.get_historic_bars(
                symbol=asset.symbol,
                timeframe='1T',
                start=start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                end=end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                limit=1,
                sort='desc',
                asset_class=asset.asset_class
            )
            
            bars = response.get('bars', [])
            if bars:
                price = Decimal(str(bars[0]['c']))  # Close price
                logger.info(f"Fetched live price from Alpaca for {asset.symbol}: ${price}")
                return price
                
        except Exception as e:
            logger.error(f"Failed to fetch price from Alpaca for {asset.symbol}: {e}")
        
        logger.warning(f"Could not determine price for {asset.symbol}")
        return None
    
    def simulate_market_order(
        self,
        asset: Asset,
        direction: str,
        quantity: Decimal,
        current_price: Optional[Decimal] = None
    ) -> Tuple[bool, str, Optional[Decimal]]:
        """
        Simulate execution of a market order.
        
        Returns:
            (success, message, execution_price)
        """
        if current_price is None:
            current_price = self.get_current_price(asset)
        
        if current_price is None:
            return False, "Unable to determine current market price", None
        
        # Apply slippage simulation
        if direction == 'LONG':
            # Buying - price slightly higher (unfavorable)
            execution_price = current_price * (Decimal('1') + self.SLIPPAGE_PERCENTAGE)
        else:
            # Selling/Shorting - price slightly lower (unfavorable)
            execution_price = current_price * (Decimal('1') - self.SLIPPAGE_PERCENTAGE)
        
        # Round to 2 decimal places
        execution_price = execution_price.quantize(Decimal('0.01'))
        
        logger.info(
            f"Simulated market order: {asset.symbol} {direction} "
            f"qty={quantity} price=${execution_price}"
        )
        
        return True, "Order executed successfully", execution_price
    
    def simulate_limit_order(
        self,
        asset: Asset,
        direction: str,
        quantity: Decimal,
        limit_price: Decimal,
        current_price: Optional[Decimal] = None
    ) -> Tuple[bool, str, Optional[Decimal]]:
        """
        Simulate execution of a limit order.
        
        For simplicity, limit orders execute immediately if price is favorable.
        In a real system, you'd queue these and check on price updates.
        """
        if current_price is None:
            current_price = self.get_current_price(asset)
        
        if current_price is None:
            return False, "Unable to determine current market price", None
        
        # Check if limit price is favorable
        if direction == 'LONG':
            # Buy limit - execute if current price <= limit price
            if current_price <= limit_price:
                execution_price = min(current_price, limit_price)
                return True, "Limit order executed", execution_price
            else:
                return False, "Price above limit - order would be queued", None
        else:
            # Sell/Short limit - execute if current price >= limit price
            if current_price >= limit_price:
                execution_price = max(current_price, limit_price)
                return True, "Limit order executed", execution_price
            else:
                return False, "Price below limit - order would be queued", None
    
    @transaction.atomic
    def execute_trade(
        self,
        asset: Asset,
        direction: str,
        quantity: Decimal,
        order_type: str = 'MARKET',
        limit_price: Optional[Decimal] = None,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        notes: str = ""
    ) -> Tuple[bool, str, Optional[PaperTrade]]:
        """
        Execute a simulated trade with full validation.
        
        Args:
            asset: Asset to trade
            direction: 'LONG' or 'SHORT'
            quantity: Number of shares
            order_type: 'MARKET' or 'LIMIT'
            limit_price: Limit price (required for LIMIT orders)
            stop_loss: Optional stop loss price
            take_profit: Optional take profit price
            notes: Optional trade notes
        
        Returns:
            (success, message, trade_object)
        """
        # Validate account can trade
        if not self.account.can_trade():
            return False, f"Account status {self.account.status} cannot place trades", None
        
        # Get current price
        current_price = self.get_current_price(asset)
        if current_price is None:
            return False, "Unable to fetch current market price", None
        
        # Validate with rule engine BEFORE execution
        from prop_firm.services.rule_engine import TradeValidator
        validator = TradeValidator(self.account)
        can_trade, errors = validator.can_place_trade(
            asset=asset,
            direction=direction,
            quantity=quantity,
            price=current_price
        )
        
        if not can_trade:
            error_msg = "; ".join(errors)
            logger.warning(f"Trade validation failed for {self.account.account_number}: {error_msg}")
            return False, error_msg, None
        
        # Simulate order execution
        if order_type == 'MARKET':
            success, msg, execution_price = self.simulate_market_order(
                asset, direction, quantity, current_price
            )
        elif order_type == 'LIMIT':
            if limit_price is None:
                return False, "Limit price required for limit orders", None
            success, msg, execution_price = self.simulate_limit_order(
                asset, direction, quantity, limit_price, current_price
            )
        else:
            return False, f"Unsupported order type: {order_type}", None
        
        if not success:
            return False, msg, None
        
        # Create trade record
        trade = PaperTrade.objects.create(
            user=self.account.user,
            asset=asset,
            direction=direction,
            quantity=quantity,
            entry_price=execution_price,
            entry_at=timezone.now(),
            target_price=take_profit,
            stop_loss=stop_loss,
            take_profit=take_profit,
            status='OPEN',
            notes=notes
        )
        
        logger.info(
            f"Trade executed: {self.account.account_number} "
            f"{direction} {quantity} {asset.symbol} @ ${execution_price}"
        )
        
        # Update account balance (deduct margin if needed)
        # For now, we don't lock funds since it's simulation
        # In production, you'd deduct buying power
        
        return True, "Trade executed successfully", trade
    
    @transaction.atomic
    def close_trade(
        self,
        trade: PaperTrade,
        exit_price: Optional[Decimal] = None,
        notes: str = ""
    ) -> Tuple[bool, str]:
        """
        Close an open trade at current market price or specified price.
        
        Args:
            trade: Open trade to close
            exit_price: Override price (for testing), uses current price if None
            notes: Optional closing notes
        
        Returns:
            (success, message)
        """
        if trade.status != 'OPEN':
            return False, f"Trade is not open (status: {trade.status})"
        
        # Get exit price
        if exit_price is None:
            exit_price = self.get_current_price(trade.asset)
            if exit_price is None:
                return False, "Unable to determine current market price"
        
        # Apply slippage for market close
        if trade.direction == 'LONG':
            # Selling - price slightly lower
            exit_price = exit_price * (Decimal('1') - self.SLIPPAGE_PERCENTAGE)
        else:
            # Covering short - price slightly higher
            exit_price = exit_price * (Decimal('1') + self.SLIPPAGE_PERCENTAGE)
        
        exit_price = exit_price.quantize(Decimal('0.01'))
        
        # Close the trade
        trade.exit_price = exit_price
        trade.exit_at = timezone.now()
        trade.status = 'CLOSED'
        if notes:
            trade.notes = f"{trade.notes}\n{notes}" if trade.notes else notes
        trade.save()
        
        # Calculate P&L
        pnl = trade.realized_pl or Decimal('0')
        
        logger.info(
            f"Trade closed: {self.account.account_number} "
            f"{trade.direction} {trade.quantity} {trade.asset.symbol} "
            f"Entry=${trade.entry_price} Exit=${exit_price} P&L=${pnl}"
        )
        
        # Update account balance
        self.account.update_balance()
        
        # Check rules after trade closes
        from prop_firm.services.rule_engine import RuleEngine
        engine = RuleEngine(self.account)
        violations = engine.check_all_rules()
        
        if violations:
            logger.warning(
                f"Rule violations detected for {self.account.account_number} "
                f"after closing trade: {len(violations)} violations"
            )
        
        return True, f"Trade closed successfully with P&L: ${pnl}"


# Convenience function for use in views
def execute_simulated_trade(
    account: PropFirmAccount,
    asset: Asset,
    direction: str,
    quantity: Decimal,
    **kwargs
) -> Tuple[bool, str, Optional[PaperTrade]]:
    """
    Convenience wrapper for executing a simulated trade.
    
    Usage in views:
        success, msg, trade = execute_simulated_trade(
            account=prop_account,
            asset=asset,
            direction='LONG',
            quantity=Decimal('10'),
            stop_loss=Decimal('145.00'),
            take_profit=Decimal('155.00')
        )
    """
    engine = TradeSimulationEngine(account)
    return engine.execute_trade(asset, direction, quantity, **kwargs)


def close_simulated_trade(
    account: PropFirmAccount,
    trade: PaperTrade,
    exit_price: Optional[Decimal] = None,
    notes: str = ""
) -> Tuple[bool, str]:
    """
    Convenience wrapper for closing a simulated trade.
    """
    engine = TradeSimulationEngine(account)
    return engine.close_trade(trade, exit_price, notes)