from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from rest_framework.response import Response
from django.contrib.auth import get_user_model
from .models import PropFirmAccount, RuleViolation, PropFirmPlan
from .admin_serializers import AdminAccountSerializer, AdminRuleViolationSerializer

User = get_user_model()

class AdminAccountViewSet(viewsets.ModelViewSet):
    """ViewSet for admin prop firm account management"""
    serializer_class = AdminAccountSerializer
    permission_classes = [IsAuthenticated, IsAdminUser]
    
    def get_queryset(self):
        """Get all accounts"""
        queryset = PropFirmAccount.objects.all().order_by('-created_at')
        
        # Filter by user
        user_email = self.request.query_params.get('user_email', None)
        if user_email:
            queryset = queryset.filter(user__email__icontains=user_email)
        
        # Filter by status
        status = self.request.query_params.get('status', None)
        if status:
            queryset = queryset.filter(status=status)
            
        # Filter by stage
        stage = self.request.query_params.get('stage', None)
        if stage:
            queryset = queryset.filter(stage=stage)
            
        return queryset
    
    def list(self, request):
        """List all accounts with pagination"""
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
            
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Accounts retrieved',
            'data': serializer.data,
            'count': queryset.count()
        })


class AdminRuleViolationViewSet(viewsets.ModelViewSet):
    """ViewSet for admin rule violation management"""
    serializer_class = AdminRuleViolationSerializer
    permission_classes = [IsAuthenticated, IsAdminUser]
    
    def get_queryset(self):
        """Get all rule violations"""
        queryset = RuleViolation.objects.all().order_by('-created_at')
        
        # Filter by account
        account_id = self.request.query_params.get('account_id', None)
        if account_id:
            queryset = queryset.filter(account_id=account_id)
            
        # Filter by violation type
        violation_type = self.request.query_params.get('violation_type', None)
        if violation_type:
            queryset = queryset.filter(violation_type=violation_type)
            
        # Filter by date range
        start_date = self.request.query_params.get('start_date', None)
        end_date = self.request.query_params.get('end_date', None)
        if start_date and end_date:
            queryset = queryset.filter(created_at__range=[start_date, end_date])
            
        return queryset
    
    def list(self, request):
        """List all violations with pagination"""
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
            
        serializer = self.get_serializer(queryset, many=True)
        return Response({
            'msg': 'Rule violations retrieved',
            'data': serializer.data,
            'count': queryset.count()
        })