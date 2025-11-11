# prop_firm/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    # Public/User ViewSets
    PropFirmPlanViewSet,
    PropFirmAccountViewSet,
    CheckoutViewSet,
    StripeWebhookView,
    PayoutViewSet,
    
    # Admin ViewSets
    AdminAccountViewSet,
    AdminRuleViolationViewSet,
    AdminPlanViewSet,
    AdminPayoutViewSet,
    AdminUserViewSet,
    AdminAssetViewSet,
    AdminWatchlistViewSet,
    AdminTradeViewSet,  # NEW - Add this import
    admin_dashboard_overview,
)

# User router - for regular users
user_router = DefaultRouter()
user_router.register(r'plans', PropFirmPlanViewSet, basename='propfirm-plans')
user_router.register(r'accounts', PropFirmAccountViewSet, basename='propfirm-accounts')
user_router.register(r'checkout', CheckoutViewSet, basename='checkout')
user_router.register(r'payouts', PayoutViewSet, basename='payouts')

# Admin router - for admin users
admin_router = DefaultRouter()
admin_router.register(r'accounts', AdminAccountViewSet, basename='admin-accounts')
admin_router.register(r'violations', AdminRuleViolationViewSet, basename='admin-violations')
admin_router.register(r'plans', AdminPlanViewSet, basename='admin-plans')
admin_router.register(r'payouts', AdminPayoutViewSet, basename='admin-payouts')
admin_router.register(r'users', AdminUserViewSet, basename='admin-users')
admin_router.register(r'assets', AdminAssetViewSet, basename='admin-assets')
admin_router.register(r'watchlists', AdminWatchlistViewSet, basename='admin-watchlists')
admin_router.register(r'trades', AdminTradeViewSet, basename='admin-trades')  # NEW

urlpatterns = [
    # Webhook endpoint (must be before router includes)
    path('webhook/stripe/', StripeWebhookView.as_view(), name='stripe-webhook'),
    
    # Admin endpoints
    path('admin/dashboard/', admin_dashboard_overview, name='admin-dashboard'),
    path('admin/', include(admin_router.urls)),
    
    # User endpoints
    path('', include(user_router.urls)),
]