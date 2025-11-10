# prop_firm/views.py - PRODUCTION READY VERSION WITH DEMO MODE
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated, AllowAny, IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db import transaction
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.contrib.auth import get_user_model
from django.conf import settings
import stripe
import logging
import json
import uuid

from .models import PropFirmPlan, PropFirmAccount, RuleViolation, Payout, AccountActivity
from .serializers import (
    PropFirmPlanSerializer, PropFirmAccountSerializer,
    PropFirmAccountListSerializer, PayoutSerializer,
    PayoutRequestSerializer, CheckoutSessionSerializer,
    RuleViolationSerializer, AccountActivitySerializer
)

logger = logging.getLogger(__name__)

# Check if Stripe is configured
STRIPE_CONFIGURED = bool(
    getattr(settings, 'STRIPE_SECRET_KEY', None) and 
    settings.STRIPE_SECRET_KEY not in ['', 'your-stripe-secret-key', None]
)

if STRIPE_CONFIGURED:
    stripe.api_key = settings.STRIPE_SECRET_KEY
    logger.info("Stripe configured for production payments")
else:
    logger.warning("Stripe not configured - running in DEMO MODE")


class PropFirmPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for prop firm plans"""
    queryset = PropFirmPlan.objects.filter(is_active=True)
    serializer_class = PropFirmPlanSerializer
    permission_classes = [AllowAny]

    def list(self, request):
        queryset = self.get_queryset().order_by('starting_balance')
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Plans retrieved successfully',
            'data': serializer.data
        })

    def retrieve(self, request, pk=None):
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
        return PropFirmAccount.objects.filter(user=self.request.user)

    def get_serializer_class(self):
        if self.action == 'list':
            return PropFirmAccountListSerializer
        return PropFirmAccountSerializer

    def list(self, request):
        queryset = self.get_queryset().order_by('-created_at')
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
        account = get_object_or_404(self.get_queryset(), pk=pk)
        account.update_balance()
        serializer = self.get_serializer(account)
        return Response({
            'msg': 'Account details retrieved',
            'data': serializer.data
        })

    @action(detail=True, methods=['post'])
    def refresh_balance(self, request, pk=None):
        account = get_object_or_404(self.get_queryset(), pk=pk)
        account.update_balance()
        
        # Check for rule violations
        violations = []
        try:
            from .services.rule_engine import RuleEngine
            rule_engine = RuleEngine(account)
            violations = rule_engine.check_all_rules()
        except Exception as e:
            logger.warning(f"Rule engine check failed: {e}")
        
        serializer = self.get_serializer(account)
        return Response({
            'msg': 'Balance updated',
            'data': serializer.data,
            'violations': violations
        })

    @action(detail=True, methods=['get'])
    def statistics(self, request, pk=None):
        account = get_object_or_404(self.get_queryset(), pk=pk)
        from paper_trading.models import PaperTrade
        
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
            'total_pnl': float(account.current_balance) - float(account.starting_balance),
            'profit_earned': float(account.profit_earned or 0),
            'total_loss': float(account.total_loss or 0),
            'trading_days': account.trading_days,
            'days_active': (timezone.now() - account.activated_at).days if account.activated_at else 0,
        }
        
        return Response({'msg': 'Statistics retrieved', 'data': stats})


class AdminDashboardAPI(APIView):
    """API for admin dashboard data"""
    permission_classes = [IsAuthenticated, IsAdminUser]

    def get(self, request):
        User = get_user_model()
        data = {
            'users_count': User.objects.count(),
            'accounts_count': PropFirmAccount.objects.count(),
            'plans_count': PropFirmPlan.objects.count(),
            'payouts_count': Payout.objects.count(),
            'violations_count': RuleViolation.objects.count(),
            'recent_accounts': []
        }
        
        recent = PropFirmAccount.objects.order_by('-created_at')[:10]
        for acc in recent:
            data['recent_accounts'].append({
                'id': acc.id,
                'account_number': acc.account_number,
                'user_email': acc.user.email if acc.user else None,
                'status': acc.status,
                'created_at': acc.created_at,
            })
        
        return Response({'msg': 'Admin dashboard data', 'data': data})


class CheckoutViewSet(viewsets.ViewSet):
    """Handle Stripe checkout and demo mode account creation"""
    permission_classes = [IsAuthenticated]

    @action(detail=False, methods=['post'])
    @transaction.atomic
    def create_session(self, request):
        """Create Stripe checkout session or demo account"""
        serializer = CheckoutSessionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        plan = get_object_or_404(
            PropFirmPlan, 
            id=serializer.validated_data['plan_id'],
            is_active=True
        )
        
        # DEMO MODE - No Stripe configured
        if not STRIPE_CONFIGURED:
            logger.info(f"Creating demo account for user {request.user.id}")
            try:
                account = self._create_demo_account(request.user, plan)
                return Response({
                    'msg': 'Demo account created successfully',
                    'data': {
                        'account_id': account.id,
                        'account_number': account.account_number,
                        'session_url': None,
                        'demo_mode': True
                    }
                }, status=status.HTTP_201_CREATED)
            except Exception as e:
                logger.error(f"Demo account creation failed: {e}")
                return Response({
                    'msg': f'Failed to create demo account: {str(e)}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # PRODUCTION MODE - Stripe configured
        try:
            account, session = self._create_stripe_checkout(
                request.user, 
                plan,
                serializer.validated_data['success_url'],
                serializer.validated_data['cancel_url']
            )
            
            return Response({
                'msg': 'Checkout session created',
                'data': {
                    'session_id': session.id,
                    'session_url': session.url,
                    'account_id': account.id,
                    'account_number': account.account_number,
                    'demo_mode': False
                }
            }, status=status.HTTP_201_CREATED)
        except Exception as e:
            logger.error(f"Checkout session creation failed: {e}")
            return Response({
                'msg': 'Failed to create checkout session',
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _create_demo_account(self, user, plan):
        """Create account without payment (demo mode)"""
        account_number = f"DEMO-{user.id}-{plan.id}-{uuid.uuid4().hex[:8].upper()}"
        
        account = PropFirmAccount.objects.create(
            user=user,
            plan=plan,
            account_number=account_number,
            starting_balance=plan.starting_balance,
            current_balance=plan.starting_balance,
            status='ACTIVE',
            stage='EVALUATION',
            activated_at=timezone.now(),
            payment_completed_at=timezone.now()
        )
        
        # Create activity log
        AccountActivity.objects.create(
            account=account,
            activity_type='ACCOUNT_CREATED',
            description=f'Demo account created - No payment required'
        )
        
        logger.info(f"Demo account created: {account.account_number}")
        return account

    def _create_stripe_checkout(self, user, plan, success_url, cancel_url):
        """Create Stripe checkout session"""
        # Generate unique account number
        account_number = f"ACC-{user.id}-{plan.id}-{uuid.uuid4().hex[:8].upper()}"
        
        # Create pending account
        account = PropFirmAccount.objects.create(
            user=user,
            plan=plan,
            account_number=account_number,
            starting_balance=plan.starting_balance,
            current_balance=plan.starting_balance,
            status='PENDING',
            stage='EVALUATION'
        )
        
        # Create Stripe session
        try:
            session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                line_items=[{
                    'price_data': {
                        'currency': 'usd',
                        'product_data': {
                            'name': plan.name,
                            'description': plan.description or f'{plan.plan_type} Trading Account',
                        },
                        'unit_amount': int(float(plan.price) * 100),  # Convert to cents
                    },
                    'quantity': 1,
                }],
                mode='payment',
                success_url=success_url,
                cancel_url=cancel_url,
                metadata={
                    'user_id': str(user.id),
                    'plan_id': str(plan.id),
                    'account_id': str(account.id),
                    'account_number': account.account_number
                },
                client_reference_id=str(user.id)
            )
            
            # Store payment intent for tracking
            account.stripe_payment_intent_id = session.payment_intent
            account.save()
            
            # Create activity log
            AccountActivity.objects.create(
                account=account,
                activity_type='PAYMENT_INITIATED',
                description=f'Stripe checkout session created: {session.id}'
            )
            
            return account, session
            
        except stripe.error.StripeError as e:
            account.delete()  # Clean up failed account
            raise e

    @action(detail=False, methods=['post'])
    def verify_payment(self, request):
        """Verify payment completion"""
        account_id = request.data.get('account_id')
        payment_intent_id = request.data.get('payment_intent_id')
        
        if not account_id:
            return Response({
                'msg': 'Account ID is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        account = get_object_or_404(
            PropFirmAccount,
            id=account_id,
            user=request.user
        )
        
        # For demo accounts, already activated
        if account.status == 'ACTIVE':
            serializer = PropFirmAccountSerializer(account)
            return Response({
                'msg': 'Account is already active',
                'data': serializer.data
            })
        
        # Verify Stripe payment
        if STRIPE_CONFIGURED and payment_intent_id:
            try:
                payment_intent = stripe.PaymentIntent.retrieve(payment_intent_id)
                
                if payment_intent.status == 'succeeded':
                    account.activate()
                    account.payment_completed_at = timezone.now()
                    account.save()
                    
                    serializer = PropFirmAccountSerializer(account)
                    return Response({
                        'msg': 'Payment verified and account activated',
                        'data': serializer.data
                    })
                else:
                    return Response({
                        'msg': f'Payment status: {payment_intent.status}'
                    }, status=status.HTTP_400_BAD_REQUEST)
            except stripe.error.StripeError as e:
                logger.error(f"Payment verification failed: {e}")
                return Response({
                    'msg': 'Payment verification failed'
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response({
            'msg': 'Unable to verify payment'
        }, status=status.HTTP_400_BAD_REQUEST)


@method_decorator(csrf_exempt, name='dispatch')
class StripeWebhookView(APIView):
    """Handle Stripe webhook events"""
    permission_classes = [AllowAny]

    def post(self, request):
        """Process webhook events"""
        if not STRIPE_CONFIGURED:
            return Response({'error': 'Stripe not configured'}, status=400)
        
        payload = request.body
        sig_header = request.META.get('HTTP_STRIPE_SIGNATURE')
        webhook_secret = getattr(settings, 'STRIPE_WEBHOOK_SECRET', None)
        
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, webhook_secret
            )
        except ValueError:
            logger.error("Invalid webhook payload")
            return Response({'error': 'Invalid payload'}, status=400)
        except stripe.error.SignatureVerificationError:
            logger.error("Invalid webhook signature")
            return Response({'error': 'Invalid signature'}, status=400)
        
        # Handle the event
        if event['type'] == 'checkout.session.completed':
            session = event['data']['object']
            self._handle_checkout_completed(session)
        
        elif event['type'] == 'payment_intent.succeeded':
            payment_intent = event['data']['object']
            self._handle_payment_succeeded(payment_intent)
        
        elif event['type'] == 'payment_intent.payment_failed':
            payment_intent = event['data']['object']
            self._handle_payment_failed(payment_intent)
        
        return Response({'status': 'success'})

    def _handle_checkout_completed(self, session):
        """Handle completed checkout"""
        try:
            metadata = session.get('metadata', {})
            account_id = metadata.get('account_id')
            
            if account_id:
                account = PropFirmAccount.objects.get(id=account_id)
                if session.payment_status == 'paid':
                    account.activate()
                    account.payment_completed_at = timezone.now()
                    account.save()
                    
                    AccountActivity.objects.create(
                        account=account,
                        activity_type='PAYMENT_COMPLETED',
                        description='Payment completed via Stripe webhook'
                    )
                    
                    logger.info(f"Account {account.account_number} activated via webhook")
        except PropFirmAccount.DoesNotExist:
            logger.error(f"Account not found for session {session.id}")
        except Exception as e:
            logger.error(f"Error handling checkout completion: {e}")

    def _handle_payment_succeeded(self, payment_intent):
        """Handle successful payment"""
        try:
            account = PropFirmAccount.objects.get(
                stripe_payment_intent_id=payment_intent.id
            )
            if account.status == 'PENDING':
                account.activate()
                account.payment_completed_at = timezone.now()
                account.save()
                logger.info(f"Payment succeeded for account {account.account_number}")
        except PropFirmAccount.DoesNotExist:
            logger.warning(f"No account found for payment intent {payment_intent.id}")

    def _handle_payment_failed(self, payment_intent):
        """Handle failed payment"""
        try:
            account = PropFirmAccount.objects.get(
                stripe_payment_intent_id=payment_intent.id
            )
            account.status = 'FAILED'
            account.failure_reason = "Payment failed"
            account.save()
            
            AccountActivity.objects.create(
                account=account,
                activity_type='PAYMENT_FAILED',
                description='Payment failed via Stripe'
            )
            
            logger.warning(f"Payment failed for account {account.account_number}")
        except PropFirmAccount.DoesNotExist:
            logger.warning(f"No account found for failed payment {payment_intent.id}")


class PayoutViewSet(viewsets.ModelViewSet):
    """ViewSet for payout management"""
    serializer_class = PayoutSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return Payout.objects.filter(account__user=user).order_by('-requested_at')

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
        
        # Validate payout eligibility
        if account.stage != 'FUNDED':
            return Response({
                'msg': 'Account must be in FUNDED stage to request payout'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        if account.profit_earned <= 0:
            return Response({
                'msg': 'No profit available for payout'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Create payout request
        payout = Payout.objects.create(
            account=account,
            profit_earned=account.profit_earned,
            profit_split=account.plan.profit_split,
            payment_method=serializer.validated_data['payment_method'],
            payment_details=serializer.validated_data.get('payment_details', {}),
            status='PENDING'
        )
        payout.calculate_amount()
        payout.save()
        
        return Response({
            'msg': 'Payout requested successfully',
            'data': PayoutSerializer(payout).data
        }, status=status.HTTP_201_CREATED)