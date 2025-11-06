from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    PropFirmPlanViewSet, PropFirmAccountViewSet,
    CheckoutViewSet, StripeWebhookView, PayoutViewSet,
    AdminDashboardAPI,
)
from .admin_views import AdminAccountViewSet, AdminRuleViolationViewSet

router = DefaultRouter()
router.register(r'plans', PropFirmPlanViewSet, basename='propfirm-plans')
router.register(r'accounts', PropFirmAccountViewSet, basename='propfirm-accounts')
router.register(r'checkout', CheckoutViewSet, basename='checkout')
router.register(r'payouts', PayoutViewSet, basename='payouts')
router.register(r'admin/accounts', AdminAccountViewSet, basename='admin-accounts')
router.register(r'admin/violations', AdminRuleViolationViewSet, basename='admin-violations')

urlpatterns = [
    path('', include(router.urls)),
    path('admin/dashboard/', AdminDashboardAPI.as_view(), name='propfirm-admin-dashboard'),
    path('webhook/stripe/', StripeWebhookView.as_view(), name='stripe-webhook'),
]