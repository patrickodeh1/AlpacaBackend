"""
URL configuration for alpacabackend project - Prop Firm Edition
"""
from django.contrib import admin
from django.urls import path, include
from prop_firm import admin as prop_admin
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    # Custom admin dashboard for prop firm superusers
    path('admin/prop-firm-dashboard/', admin.site.admin_view(prop_admin.admin_dashboard), name='prop_firm_admin_dashboard'),
    
    # API endpoints
    path("api/account/", include("account.urls")),
    path("api/core/", include("core.urls")),
    path("api/paper-trading/", include("paper_trading.urls")),
    path("api/prop-firm/", include("prop_firm.urls")),  # New prop firm endpoints
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)