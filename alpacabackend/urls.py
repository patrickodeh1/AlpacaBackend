"""
URL configuration for alpacabackend project - Prop Firm Edition
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    
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