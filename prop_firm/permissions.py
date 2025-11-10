# prop_firm/permissions.py
from rest_framework import permissions

class IsAdminUser(permissions.BasePermission):
    """
    Custom permission to check for admin status
    Supports both Django's is_staff and custom is_admin field
    """
    def has_permission(self, request, view):
        return bool(
            request.user and 
            request.user.is_authenticated and 
            (getattr(request.user, 'is_admin', False) or 
             getattr(request.user, 'is_staff', False) or
             getattr(request.user, 'is_superuser', False))
        )