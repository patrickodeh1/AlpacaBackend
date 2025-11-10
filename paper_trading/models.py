# paper_trading/models.py
"""
Updated PaperTrade model with order state management for simulation.
"""

from decimal import Decimal
from django.conf import settings
from django.db import models
from django.utils import timezone


class PaperTrade(models.Model):
    """
    Simulated trading position with enhanced order state tracking.
    """

    class Direction(models.TextChoices):
        LONG = "LONG", "Long"
        SHORT = "SHORT", "Short"

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"  # Order submitted but not filled
        OPEN = "OPEN", "Open"  # Position is open
        CLOSED = "CLOSED", "Closed"  # Position closed
        CANCELLED = "CANCELLED", "Cancelled"  # Order cancelled before fill
        REJECTED = "REJECTED", "Rejected"  # Order rejected (e.g., insufficient funds)

    class OrderType(models.TextChoices):
        MARKET = "MARKET", "Market Order"
        LIMIT = "LIMIT", "Limit Order"
        STOP = "STOP", "Stop Order"
        STOP_LIMIT = "STOP_LIMIT", "Stop-Limit Order"

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="paper_trades",
    )
    asset = models.ForeignKey(
        "core.Asset",
        on_delete=models.CASCADE,
        related_name="paper_trades",
    )
    
    # Order details
    direction = models.CharField(
        max_length=5,
        choices=Direction.choices,
        default=Direction.LONG,
    )
    quantity = models.DecimalField(max_digits=18, decimal_places=6)
    order_type = models.CharField(
        max_length=15,
        choices=OrderType.choices,
        default=OrderType.MARKET,
    )
    
    # Execution details
    entry_price = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        help_text="Actual fill price"
    )
    limit_price = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Limit price for limit orders"
    )
    entry_at = models.DateTimeField(default=timezone.now)
    
    # Risk management
    target_price = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Target price (informational)",
    )
    stop_loss = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Stop loss price",
    )
    take_profit = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Take profit price",
    )
    
    # Status and closing
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.OPEN,
    )
    exit_price = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
    )
    exit_at = models.DateTimeField(null=True, blank=True)
    
    # Simulation metadata
    commission = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        default=Decimal('0.00'),
        help_text="Simulated commission charged"
    )
    slippage = models.DecimalField(
        max_digits=10,
        decimal_places=6,
        default=Decimal('0.00'),
        help_text="Simulated slippage applied"
    )
    
    # Notes and tracking
    notes = models.TextField(blank=True)
    rejection_reason = models.TextField(
        blank=True,
        help_text="Reason for rejection if status is REJECTED"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(
                fields=["user", "asset", "status"],
                name="pt_user_asset_status_idx",
            ),
            models.Index(
                fields=["asset", "entry_at"],
                name="pt_asset_entry_idx",
            ),
            models.Index(
                fields=["user", "status", "-created_at"],
                name="pt_user_status_created_idx",
            ),
        ]

    def __str__(self) -> str:
        return (
            f"{self.user_id}:{self.asset.symbol} {self.direction} "
            f"{self.quantity}@{self.entry_price} [{self.status}]"
        )

    @property
    def entry_cost(self) -> Decimal:
        """Total cost to enter position (excluding commission)"""
        return (self.entry_price or Decimal("0")) * (self.quantity or Decimal("0"))

    @property
    def is_open(self) -> bool:
        """Check if position is currently open"""
        return self.status == self.Status.OPEN

    @property
    def realized_pl(self) -> Decimal | None:
        """Calculate realized P&L for closed positions"""
        if self.exit_price is None:
            return None
        return self._compute_pl(self.exit_price) - self.commission

    def _compute_pl(self, price: Decimal) -> Decimal:
        """Calculate P&L at given price (before commission)"""
        multiplier = (
            Decimal("1") if self.direction == self.Direction.LONG else Decimal("-1")
        )
        return (price - self.entry_price) * self.quantity * multiplier

    def compute_unrealized_pl(self, current_price: Decimal | None) -> Decimal | None:
        """Calculate unrealized P&L at current market price"""
        if current_price is None:
            return None
        return self._compute_pl(current_price) - self.commission
    
    def compute_pl_percentage(self, current_price: Decimal | None = None) -> Decimal | None:
        """Calculate P&L as percentage of entry cost"""
        if current_price is None and self.exit_price is None:
            return None
        
        price = self.exit_price if self.exit_price else current_price
        if price is None or self.entry_cost == 0:
            return None
        
        pl = self._compute_pl(price) - self.commission
        return (pl / self.entry_cost) * Decimal('100')

    def mark_closed(self, *, exit_price: Decimal, exit_at=None, notes: str = "") -> None:
        """Close the position"""
        if not self.is_open:
            raise ValueError("Trade is not open and cannot be closed.")
        
        self.exit_price = exit_price
        self.exit_at = exit_at or timezone.now()
        self.status = self.Status.CLOSED
        
        if notes:
            self.notes = f"{self.notes}\n{notes}" if self.notes else notes
        
        self.save(update_fields=["exit_price", "exit_at", "status", "notes", "updated_at"])

    def cancel(self, reason: str = "") -> None:
        """Cancel pending order"""
        if self.status not in [self.Status.PENDING, self.Status.OPEN]:
            raise ValueError(f"Cannot cancel trade with status {self.status}")
        
        self.status = self.Status.CANCELLED
        self.exit_at = timezone.now()
        
        if reason:
            self.rejection_reason = reason
        
        self.save(update_fields=["status", "exit_at", "rejection_reason", "updated_at"])
    
    def reject(self, reason: str) -> None:
        """Reject order (e.g., insufficient funds, rule violation)"""
        self.status = self.Status.REJECTED
        self.rejection_reason = reason
        self.save(update_fields=["status", "rejection_reason", "updated_at"])