import stripe
from django.conf import settings
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

stripe.api_key = settings.STRIPE_SECRET_KEY


class StripeService:
    """Handle Stripe payment operations"""
    
    @staticmethod
    def create_payment_intent(amount, currency='usd', metadata=None):
        """Create a payment intent for account purchase"""
        try:
            intent = stripe.PaymentIntent.create(
                amount=int(amount * 100),  # Convert to cents
                currency=currency,
                metadata=metadata or {},
                automatic_payment_methods={'enabled': True},
            )
            return intent
        except stripe.error.StripeError as e:
            logger.error(f"Stripe payment intent creation failed: {e}")
            raise
    
    @staticmethod
    def confirm_payment(payment_intent_id):
        """Confirm payment was successful"""
        try:
            intent = stripe.PaymentIntent.retrieve(payment_intent_id)
            return intent.status == 'succeeded'
        except stripe.error.StripeError as e:
            logger.error(f"Failed to confirm payment: {e}")
            return False
    
    @staticmethod
    def create_customer(email, name, metadata=None):
        """Create a Stripe customer"""
        try:
            customer = stripe.Customer.create(
                email=email,
                name=name,
                metadata=metadata or {}
            )
            return customer
        except stripe.error.StripeError as e:
            logger.error(f"Failed to create Stripe customer: {e}")
            raise
    
    @staticmethod
    def create_payout(amount, destination, metadata=None):
        """Create a payout to trader"""
        try:
            # This requires Stripe Connect setup
            transfer = stripe.Transfer.create(
                amount=int(amount * 100),
                currency='usd',
                destination=destination,
                metadata=metadata or {}
            )
            return transfer
        except stripe.error.StripeError as e:
            logger.error(f"Failed to create payout: {e}")
            raise
    
    @staticmethod
    def handle_webhook(payload, sig_header):
        """Handle Stripe webhook events"""
        try:
            event = stripe.Webhook.construct_event(
                payload, 
                sig_header, 
                settings.STRIPE_WEBHOOK_SECRET
            )
            return event
        except ValueError:
            logger.error("Invalid payload")
            raise
        except stripe.error.SignatureVerificationError:
            logger.error("Invalid signature")
            raise
    
    @staticmethod
    def create_checkout_session(plan, success_url, cancel_url, user_email, metadata=None):
        """Create Stripe checkout session for plan purchase"""
        try:
            session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                line_items=[{
                    'price_data': {
                        'currency': 'usd',
                        'unit_amount': int(plan.price * 100),
                        'product_data': {
                            'name': plan.name,
                            'description': plan.description,
                        },
                    },
                    'quantity': 1,
                }],
                mode='payment',
                success_url=success_url,
                cancel_url=cancel_url,
                customer_email=user_email,
                metadata=metadata or {}
            )
            return session
        except stripe.error.StripeError as e:
            logger.error(f"Failed to create checkout session: {e}")
            raise
    
    @staticmethod
    def get_payment_intent(payment_intent_id):
        """Retrieve payment intent details"""
        try:
            return stripe.PaymentIntent.retrieve(payment_intent_id)
        except stripe.error.StripeError as e:
            logger.error(f"Failed to retrieve payment intent: {e}")
            raise
    
    @staticmethod
    def refund_payment(payment_intent_id, amount=None):
        """Refund a payment"""
        try:
            refund_params = {'payment_intent': payment_intent_id}
            if amount:
                refund_params['amount'] = int(amount * 100)
            
            refund = stripe.Refund.create(**refund_params)
            return refund
        except stripe.error.StripeError as e:
            logger.error(f"Failed to create refund: {e}")
            raise


class PaymentProcessor:
    """Process payments for prop firm accounts"""
    
    def __init__(self, user, plan):
        self.user = user
        self.plan = plan
        self.stripe_service = StripeService()
    
    def create_account_purchase(self, success_url, cancel_url):
        """Create payment session for account purchase"""
        from prop_firm.models import PropFirmAccount, AccountActivity
        
        # Create pending account
        account = PropFirmAccount.objects.create(
            user=self.user,
            plan=self.plan,
            status='PENDING',
            stage='EVALUATION' if self.plan.plan_type == 'EVALUATION' else 'FUNDED'
        )
        
        # Create Stripe checkout session
        metadata = {
            'user_id': str(self.user.id),
            'account_id': str(account.id),
            'plan_id': str(self.plan.id),
        }
        
        session = self.stripe_service.create_checkout_session(
            plan=self.plan,
            success_url=success_url,
            cancel_url=cancel_url,
            user_email=self.user.email,
            metadata=metadata
        )
        
        # Store session info
        account.stripe_payment_intent_id = session.payment_intent
        account.save()
        
        # Log activity
        AccountActivity.objects.create(
            account=account,
            activity_type='CREATED',
            description=f'Account created pending payment of ${self.plan.price}',
            created_by=self.user
        )
        
        return account, session
    
    def complete_payment(self, account, payment_intent_id):
        """Complete payment and activate account"""
        from prop_firm.models import AccountActivity
        from django.utils import timezone
        
        # Verify payment
        if self.stripe_service.confirm_payment(payment_intent_id):
            account.status = 'ACTIVE'
            account.activated_at = timezone.now()
            account.payment_completed_at = timezone.now()
            account.save()
            
            # Log activity
            AccountActivity.objects.create(
                account=account,
                activity_type='ACTIVATED',
                description=f'Account activated after successful payment of ${self.plan.price}',
                metadata={'payment_intent': payment_intent_id}
            )
            
            logger.info(f"Account {account.account_number} activated for user {self.user.email}")
            return True
        
        logger.error(f"Payment verification failed for account {account.id}")
        return False