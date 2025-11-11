# paper_trading/views.py - INTEGRATED WITH PROP FIRM SIMULATION
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from decimal import Decimal
import logging

from .models import PaperTrade
from .serializers import PaperTradeSerializer
from core.models import Asset
from prop_firm.models import PropFirmAccount
from prop_firm.services.trade_simulation import (
    TradeSimulationEngine,
    execute_simulated_trade,
    close_simulated_trade
)

logger = logging.getLogger(__name__)


class PaperTradeViewSet(viewsets.ModelViewSet):
    """
    ViewSet for paper trading with prop firm simulation integration
    """
    serializer_class = PaperTradeSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        """Filter trades by user"""
        return PaperTrade.objects.filter(user=self.request.user).order_by('-created_at')


    @action(detail=True, methods=['patch'], url_path='update')
    def update_trade(self, request, pk=None):
        """
        Update stop loss and take profit for an open trade.
        Only allows updating SL/TP, not other fields.
        """
        try:
            trade = self.get_object()
            
            # Only allow updates to OPEN trades
            if trade.status != 'OPEN':
                return Response(
                    {"msg": "Can only update open trades"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get update fields
            stop_loss = request.data.get('stop_loss')
            take_profit = request.data.get('take_profit')
            
            # Update stop loss if provided
            if stop_loss is not None:
                if stop_loss == '' or stop_loss == 'null':
                    trade.stop_loss = None
                else:
                    try:
                        sl_value = Decimal(str(stop_loss))
                        
                        # Validate stop loss makes sense for trade direction
                        if trade.direction == 'LONG' and sl_value >= trade.entry_price:
                            return Response(
                                {"msg": "Stop loss must be below entry price for long positions"},
                                status=status.HTTP_400_BAD_REQUEST
                            )
                        elif trade.direction == 'SHORT' and sl_value <= trade.entry_price:
                            return Response(
                                {"msg": "Stop loss must be above entry price for short positions"},
                                status=status.HTTP_400_BAD_REQUEST
                            )
                        
                        trade.stop_loss = sl_value
                    except (ValueError, TypeError):
                        return Response(
                            {"msg": "Invalid stop loss value"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
            
            # Update take profit if provided
            if take_profit is not None:
                if take_profit == '' or take_profit == 'null':
                    trade.take_profit = None
                else:
                    try:
                        tp_value = Decimal(str(take_profit))
                        
                        # Validate take profit makes sense for trade direction
                        if trade.direction == 'LONG' and tp_value <= trade.entry_price:
                            return Response(
                                {"msg": "Take profit must be above entry price for long positions"},
                                status=status.HTTP_400_BAD_REQUEST
                            )
                        elif trade.direction == 'SHORT' and tp_value >= trade.entry_price:
                            return Response(
                                {"msg": "Take profit must be below entry price for short positions"},
                                status=status.HTTP_400_BAD_REQUEST
                            )
                        
                        trade.take_profit = tp_value
                    except (ValueError, TypeError):
                        return Response(
                            {"msg": "Invalid take profit value"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
            
            trade.save()
            
            serializer = self.get_serializer(trade)
            return Response(
                {"msg": "Trade updated successfully", "data": serializer.data},
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            logger.error(f"Error updating trade: {e}", exc_info=True)
            return Response(
                {"msg": "Error updating trade", "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def list(self, request):
        """
        List all trades for the current user.
        Optionally filter by asset and update unrealized P&L with current price.
        """
        queryset = self.get_queryset()
        
        # Filter by asset if provided
        asset_id = request.query_params.get('asset')
        if asset_id:
            queryset = queryset.filter(asset_id=asset_id)
        
        # Filter by status if provided
        trade_status = request.query_params.get('status')
        if trade_status:
            queryset = queryset.filter(status=trade_status)
        
        # Update unrealized P&L with current price if provided
        current_price = request.query_params.get('current_price')
        if current_price:
            try:
                current_price_decimal = Decimal(str(current_price))
                for trade in queryset.filter(status='OPEN'):
                    # Calculate unrealized P&L
                    entry_price = Decimal(str(trade.entry_price))
                    quantity = Decimal(str(trade.quantity))
                    
                    if trade.direction == 'LONG':
                        unrealized_pl = (current_price_decimal - entry_price) * quantity
                    else:  # SHORT
                        unrealized_pl = (entry_price - current_price_decimal) * quantity
                    
                    trade.unrealized_pl = unrealized_pl
                    trade.current_value = current_price_decimal * quantity
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid current_price parameter: {e}")
        
        serializer = self.get_serializer(queryset, many=True)
        
        return Response({
            'msg': 'Paper trades retrieved successfully',
            'data': serializer.data
        })

    def create(self, request):
        """
        Create a new paper trade with prop firm simulation integration.
        
        If user has an active PropFirmAccount, use TradeSimulationEngine.
        Otherwise, create a standard paper trade.
        """
        # Log the incoming data for debugging
        logger.info(f"Received trade request: {request.data}")
        
        # Validate required fields
        required_fields = ['asset', 'direction', 'quantity', 'entry_price']
        missing_fields = [field for field in required_fields if field not in request.data]
        
        if missing_fields:
            return Response({
                'msg': f'Missing required fields: {", ".join(missing_fields)}',
                'error': 'VALIDATION_ERROR'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # Parse and validate data
            asset_id = int(request.data['asset'])
            direction = str(request.data['direction']).upper()
            quantity = Decimal(str(request.data['quantity']))
            entry_price = Decimal(str(request.data['entry_price']))
            
            # Validate direction
            if direction not in ['LONG', 'SHORT']:
                return Response({
                    'msg': f'Invalid direction: {direction}. Must be LONG or SHORT',
                    'error': 'VALIDATION_ERROR'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate positive values
            if quantity <= 0:
                return Response({
                    'msg': 'Quantity must be greater than 0',
                    'error': 'VALIDATION_ERROR'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            if entry_price <= 0:
                return Response({
                    'msg': 'Entry price must be greater than 0',
                    'error': 'VALIDATION_ERROR'
                }, status=status.HTTP_400_BAD_REQUEST)
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Invalid trade data: {e}")
            return Response({
                'msg': f'Invalid trade data: {str(e)}',
                'error': 'VALIDATION_ERROR'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get asset
        try:
            asset = Asset.objects.get(id=asset_id)
        except Asset.DoesNotExist:
            return Response({
                'msg': f'Asset with id {asset_id} not found',
                'error': 'ASSET_NOT_FOUND'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Parse optional fields
        stop_loss = None
        take_profit = None
        notes = request.data.get('notes', '')
        
        if request.data.get('stop_loss'):
            try:
                stop_loss = Decimal(str(request.data['stop_loss']))
            except (ValueError, TypeError):
                logger.warning(f"Invalid stop_loss value: {request.data.get('stop_loss')}")
        
        if request.data.get('take_profit'):
            try:
                take_profit = Decimal(str(request.data['take_profit']))
            except (ValueError, TypeError):
                logger.warning(f"Invalid take_profit value: {request.data.get('take_profit')}")
        
        # Check for active prop firm account
        active_account = PropFirmAccount.objects.filter(
            user=request.user,
            status='ACTIVE',
            stage__in=['EVALUATION', 'FUNDED']
        ).first()
        
        if active_account:
            # USE PROP FIRM SIMULATION ENGINE
            logger.info(f"Creating trade for prop firm account: {active_account.account_number}")
            
            success, message, trade = execute_simulated_trade(
                account=active_account,
                asset=asset,
                direction=direction,
                quantity=quantity,
                order_type='MARKET',
                stop_loss=stop_loss,
                take_profit=take_profit,
                notes=notes
            )
            
            if not success:
                logger.warning(f"Trade execution failed: {message}")
                return Response({
                    'msg': message,
                    'error': 'EXECUTION_FAILED'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Update account balance after trade
            active_account.update_balance()
            
            serializer = PaperTradeSerializer(trade)
            return Response({
                'msg': 'Trade executed successfully via prop firm simulation',
                'data': serializer.data,
                'prop_firm_account': {
                    'account_number': active_account.account_number,
                    'current_balance': float(active_account.current_balance),
                    'status': active_account.status
                }
            }, status=status.HTTP_201_CREATED)
        
        else:
            # STANDARD PAPER TRADE (No prop firm account)
            logger.info(f"Creating standard paper trade for user: {request.user.id}")
            
            try:
                trade = PaperTrade.objects.create(
                    user=request.user,
                    asset=asset,
                    direction=direction,
                    quantity=str(quantity),
                    entry_price=str(entry_price),
                    stop_loss=str(stop_loss) if stop_loss else None,
                    take_profit=str(take_profit) if take_profit else None,
                    notes=notes,
                    status='OPEN'
                )
                
                serializer = PaperTradeSerializer(trade)
                return Response({
                    'msg': 'Paper trade created successfully',
                    'data': serializer.data
                }, status=status.HTTP_201_CREATED)
            
            except Exception as e:
                logger.error(f"Failed to create paper trade: {e}")
                return Response({
                    'msg': f'Failed to create trade: {str(e)}',
                    'error': 'DATABASE_ERROR'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=True, methods=['post'])
    def close(self, request, pk=None):
        """
        Close a paper trade with prop firm simulation integration.
        """
        try:
            trade = self.get_object()
            
            # Only allow closing OPEN trades
            if trade.status != 'OPEN':
                return Response(
                    {"msg": "Trade is already closed"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Get exit price from request
            exit_price = request.data.get('exit_price')
            if not exit_price:
                return Response(
                    {"msg": "Exit price is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            try:
                exit_price = Decimal(str(exit_price))
            except (ValueError, TypeError):
                return Response(
                    {"msg": "Invalid exit price"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Calculate realized P/L
            entry_price = trade.entry_price
            quantity = trade.quantity
            
            if trade.direction == 'LONG':
                realized_pl = (exit_price - entry_price) * quantity
            else:  # SHORT
                realized_pl = (entry_price - exit_price) * quantity
            
            # Update trade
            trade.exit_price = exit_price
            trade.exit_time = timezone.now()
            trade.realized_pl = realized_pl
            trade.status = 'CLOSED'
            trade.save()
            
            # Update account balance
            account = trade.account
            account.current_balance = Decimal(str(account.current_balance)) + realized_pl
            account.profit_earned = Decimal(str(account.profit_earned)) + realized_pl
            
            # Update trade counters
            if realized_pl >= 0:
                account.total_winning_trades += 1
            else:
                account.total_losing_trades += 1
            
            account.save()
            
            serializer = self.get_serializer(trade)
            return Response(
                {
                    "msg": "Trade closed successfully",
                    "data": serializer.data,
                    "realized_pl": float(realized_pl),
                    "new_balance": float(account.current_balance)
                },
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            logger.error(f"Error closing trade: {e}", exc_info=True)
            return Response(
                {"msg": "Error closing trade", "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    @action(detail=True, methods=['post'])
    def cancel(self, request, pk=None):
        """Cancel a trade"""
        trade = get_object_or_404(self.get_queryset(), pk=pk)
        
        if trade.status != 'OPEN':
            return Response({
                'msg': f'Trade is not open (status: {trade.status})'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        trade.status = 'CANCELLED'
        notes = request.data.get('notes', '')
        if notes:
            trade.notes = f"{trade.notes}\n{notes}" if trade.notes else notes
        trade.save()
        
        serializer = PaperTradeSerializer(trade)
        return Response({
            'msg': 'Trade cancelled successfully',
            'data': serializer.data
        })

    def destroy(self, request, pk=None):
        """Delete a trade"""
        trade = get_object_or_404(self.get_queryset(), pk=pk)
        trade.delete()
        
        return Response({
            'msg': 'Trade deleted successfully'
        }, status=status.HTTP_204_NO_CONTENT)

