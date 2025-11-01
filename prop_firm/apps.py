from django.apps import AppConfig


class PropFirmConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prop_firm'
    verbose_name = 'Prop Trading Firm'
    
    def ready(self):
        """Import signals when app is ready"""
        import prop_firm.signals  # noqa