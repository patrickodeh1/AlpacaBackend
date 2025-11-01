from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    PropFirmPlanViewSet, PropFirmAccountViewSet,
    CheckoutViewSet, StripeWebhookView, PayoutViewSet
)

router = DefaultRouter()
router.register(r'plans', PropFirmPlanViewSet, basename='propfirm-plans')
router.register(r'accounts', PropFirmAccountViewSet, basename='propfirm-accounts')
router.register(r'checkout', CheckoutViewSet, basename='checkout')
router.register(r'payouts', PayoutViewSet, basename='payouts')

urlpatterns = [
    path('', include(router.urls)),
    path('webhook/stripe/', StripeWebhookView.as_view(), name='stripe-webhook'),
]