from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db.models import Q
import logging

from .models import (
    PropFirmPlan, PropFirmAccount, RuleViolation,
    Payout, AccountActivity
)
from .serializers import (
    PropFirmPlanSerializer, PropFirmAccountSerializer,
    PropFirmAccountListSerializer, PayoutSerializer,
    PayoutRequestSerializer, CheckoutSessionSerializer,
    RuleViolationSerializer, AccountActivitySerializer
)
from .services.stripe_service import StripeService, PaymentProcessor
from .services.rule_engine import RuleEngine

logger = logging.getLogger(__name__)


class PropFirmPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for prop firm plans"""
    
    queryset = PropFirmPlan.objects.filter(is_active=True)
    serializer_class = PropFirmPlanSerializer
    permission_classes = [AllowAny]
    
    def list(self, request):
        """List all active plans"""
        queryset = self.get_queryset().order_by('starting_balance')
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Plans retrieved successfully',
            'data': serializer.data
        })
    
    def retrieve(self, request, pk=None):
        """Get plan details"""
        plan = get_object_or_404(self.queryset, pk=pk)
        serializer = self.get_serializer(plan)
        return Response({
            'msg': 'Plan details retrieved',
            'data': serializer.data
        })


class PropFirmAccountViewSet(viewsets.ModelViewSet):
    """ViewSet for prop firm accounts"""
    
    serializer_class = PropFirmAccountSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Get accounts for current user"""
        return PropFirmAccount.objects.filter(user=self.request.user)
    
    def get_serializer_class(self):
        """Use different serializers for list vs detail"""
        if self.action == 'list':
            return PropFirmAccountListSerializer
        return PropFirmAccountSerializer
    
    def list(self, request):
        """List user's accounts"""
        queryset = self.get_queryset().order_by('-created_at')
        
        # Filter by status
        status_filter = request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Accounts retrieved successfully',
            'data': serializer.data,
            'count': queryset.count()
        })
    
    def retrieve(self, request, pk=None):
        """Get account details"""
        account = get_object_or_404(self.get_queryset(), pk=pk)
        
        # Update balance before returning
        account.update_balance()
        
        serializer = self.get_serializer(account)
        return Response({
            'msg': 'Account details retrieved',
            'data': serializer.data
        })
    
    @action(detail=True, methods=['post'])
    def refresh_balance(self, request, pk=None):
        """Manually refresh account balance"""
        account = get_object_or_404(self.get_queryset(), pk=pk)
        account.update_balance()
        
        # Check rules
        rule_engine = RuleEngine(account)
        violations = rule_engine.check_all_rules()
        
        serializer = self.get_serializer(account)
        return Response({
            'msg': 'Balance updated',
            'data': serializer.data,
            'violations': violations
        })
    
    @action(detail=True, methods=['get'])
    def activities(self, request, pk=None):
        """Get account activities"""
        account = get_object_or_404(self.get_queryset(), pk=pk)
        activities = account.activities.all().order_by('-created_at')
        
        # Pagination
        from rest_framework.pagination import PageNumberPagination
        paginator = PageNumberPagination()
        paginator.page_size = 20
        
        page = paginator.paginate_queryset(activities, request)
        if page is not None:
            serializer = AccountActivitySerializer(page, many=True)
            return paginator.get_paginated_response(serializer.data)
        
        serializer = AccountActivitySerializer(activities, many=True)
        return Response({
            'msg': 'Activities retrieved',
            'data': serializer.data
        })
    
    @action(detail=True, methods=['get'])
    def violations(self, request, pk=None):
        """Get account violations"""
        account = get_object_or_404(self.get_queryset(), pk=pk)
        violations = account.violations.all().order_by('-created_at')
        serializer = RuleViolationSerializer(violations, many=True)
        return Response({
            'msg': 'Violations retrieved',
            'data': serializer.data
        })
    
    @action(detail=True, methods=['get'])
    def statistics(self, request, pk=None):
        """Get account statistics"""
        account = get_object_or_404(self.get_queryset(), pk=pk)
        from paper_trading.models import PaperTrade
        from decimal import Decimal
        
        # Get all trades for this account
        trades = PaperTrade.objects.filter(
            user=account.user,
            created_at__gte=account.created_at
        )
        
        closed_trades = trades.filter(status='CLOSED')
        winning_trades = [t for t in closed_trades if (t.realized_pl or 0) > 0]
        losing_trades = [t for t in closed_trades if (t.realized_pl or 0) < 0]
        
        stats = {
            'total_trades': trades.count(),
            'open_trades': trades.filter(status='OPEN').count(),
            'closed_trades': closed_trades.count(),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': (len(winning_trades) / len(closed_trades) * 100) if closed_trades else 0,
            'total_pnl': account.current_balance - account.starting_balance,
            'profit_earned': account.profit_earned,
            'total_loss': account.total_loss,
            'trading_days': account.trading_days,
            'days_active': (timezone.now() - account.activated_at).days if account.activated_at else 0,
            'average_win': sum(t.realized_pl for t in winning_trades) / len(winning_trades) if winning_trades else 0,
            'average_loss': sum(t.realized_pl for t in losing_trades) / len(losing_trades) if losing_trades else 0,
        }
        
        return Response({
            'msg': 'Statistics retrieved',
            'data': stats
        })


class CheckoutViewSet(viewsets.ViewSet):
    """Handle checkout and payment flows"""
    
    permission_classes = [IsAuthenticated]
    
    @action(detail=False, methods=['post'])
    def create_session(self, request):
        """Create Stripe checkout session"""
        serializer = CheckoutSessionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        plan = get_object_or_404(PropFirmPlan, id=serializer.validated_data['plan_id'])
        
        # Create payment session
        processor = PaymentProcessor(request.user, plan)
        
        try:
            account, session = processor.create_account_purchase(
                success_url=serializer.validated_data['success_url'],
                cancel_url=serializer.validated_data['cancel_url']
            )
            
            return Response({
                'msg': 'Checkout session created',
                'data': {
                    'session_id': session.id,
                    'session_url': session.url,
                    'account_id': account.id,
                    'account_number': account.account_number
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Checkout session creation failed: {e}")
            return Response({
                'msg': 'Failed to create checkout session',
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=False, methods=['post'])
    def verify_payment(self, request):
        """Verify payment completion"""
        account_id = request.data.get('account_id')
        payment_intent_id = request.data.get('payment_intent_id')
        
        if not account_id or not payment_intent_id:
            return Response({
                'msg': 'Missing required parameters'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        account = get_object_or_404(
            PropFirmAccount,
            id=account_id,
            user=request.user
        )
        
        # Complete payment
        processor = PaymentProcessor(request.user, account.plan)
        success = processor.complete_payment(account, payment_intent_id)
        
        if success:
            account.refresh_from_db()
            serializer = PropFirmAccountSerializer(account)
            return Response({
                'msg': 'Payment verified and account activated',
                'data': serializer.data
            })
        else:
            return Response({
                'msg': 'Payment verification failed'
            }, status=status.HTTP_400_BAD_REQUEST)


class StripeWebhookView(APIView):
    """Handle Stripe webhooks"""
    
    permission_classes = [AllowAny]
    
    def post(self, request):
        """Process webhook events"""
        payload = request.body
        sig_header = request.META.get('HTTP_STRIPE_SIGNATURE')
        
        try:
            event = StripeService.handle_webhook(payload, sig_header)
        except Exception as e:
            logger.error(f"Webhook signature verification failed: {e}")
            return Response(status=status.HTTP_400_BAD_REQUEST)
        
        # Handle event
        if event['type'] == 'payment_intent.succeeded':
            self._handle_payment_success(event['data']['object'])
        elif event['type'] == 'payment_intent.payment_failed':
            self._handle_payment_failed(event['data']['object'])
        elif event['type'] == 'checkout.session.completed':
            self._handle_checkout_completed(event['data']['object'])
        
        return Response({'status': 'success'})
    
    def _handle_payment_success(self, payment_intent):
        """Handle successful payment"""
        try:
            account = PropFirmAccount.objects.get(
                stripe_payment_intent_id=payment_intent['id']
            )
            
            if account.status == 'PENDING':
                processor = PaymentProcessor(account.user, account.plan)
                processor.complete_payment(account, payment_intent['id'])
                
                logger.info(f"Payment success webhook processed for account {account.account_number}")
        except PropFirmAccount.DoesNotExist:
            logger.error(f"Account not found for payment intent {payment_intent['id']}")
    
    def _handle_payment_failed(self, payment_intent):
        """Handle failed payment"""
        try:
            account = PropFirmAccount.objects.get(
                stripe_payment_intent_id=payment_intent['id']
            )
            
            AccountActivity.objects.create(
                account=account,
                activity_type='NOTE_ADDED',
                description=f"Payment failed: {payment_intent.get('last_payment_error', {}).get('message', 'Unknown error')}"
            )
            
            logger.warning(f"Payment failed for account {account.account_number}")
        except PropFirmAccount.DoesNotExist:
            logger.error(f"Account not found for payment intent {payment_intent['id']}")
    
    def _handle_checkout_completed(self, session):
        """Handle completed checkout session"""
        metadata = session.get('metadata', {})
        account_id = metadata.get('account_id')
        
        if account_id:
            try:
                account = PropFirmAccount.objects.get(id=account_id)
                logger.info(f"Checkout completed for account {account.account_number}")
            except PropFirmAccount.DoesNotExist:
                logger.error(f"Account not found for checkout session")


class PayoutViewSet(viewsets.ModelViewSet):
    """ViewSet for payouts"""
    
    serializer_class = PayoutSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Get payouts for user's accounts"""
        return Payout.objects.filter(account__user=self.request.user)
    
    def list(self, request):
        """List user's payouts"""
        queryset = self.get_queryset().order_by('-requested_at')
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Payouts retrieved',
            'data': serializer.data
        })
    
    @action(detail=False, methods=['post'])
    def request_payout(self, request):
        """Request a payout"""
        serializer = PayoutRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        account = get_object_or_404(
            PropFirmAccount,
            id=serializer.validated_data['account_id'],
            user=request.user
        )
        
        # Create payout
        payout = Payout.objects.create(
            account=account,
            profit_earned=account.profit_earned,
            profit_split=account.plan.profit_split,
            payment_method=serializer.validated_data['payment_method'],
            payment_details=serializer.validated_data.get('payment_details', {})
        )
        
        payout.calculate_amount()
        payout.save()
        
        # Log activity
        AccountActivity.objects.create(
            account=account,
            activity_type='PAYOUT_REQUEST',
            description=f"Payout requested: ${payout.amount}",
            created_by=request.user
        )
        
        return Response({
            'msg': 'Payout requested successfully',
            'data': PayoutSerializer(payout).data
        }, status=status.HTTP_201_CREATED)