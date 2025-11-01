from django.core.management.base import BaseCommand
from prop_firm.models import PropFirmPlan
from decimal import Decimal


class Command(BaseCommand):
    help = 'Set up initial prop firm plans'

    def handle(self, *args, **options):
        self.stdout.write('Creating initial prop firm plans...')
        
        plans = [
            {
                'name': '$25K Starter Challenge',
                'description': 'Perfect for beginners. Start with $25,000 and prove your skills.',
                'plan_type': 'EVALUATION',
                'starting_balance': Decimal('25000.00'),
                'price': Decimal('49.00'),
                'max_daily_loss': Decimal('1250.00'),  # 5%
                'max_total_loss': Decimal('2500.00'),  # 10%
                'profit_target': Decimal('2500.00'),  # 10%
                'min_trading_days': 3,
                'max_position_size': Decimal('100.00'),
                'profit_split': Decimal('80.00'),
            },
            {
                'name': '$50K Challenge',
                'description': 'Prove your trading skills with a $50,000 account',
                'plan_type': 'EVALUATION',
                'starting_balance': Decimal('50000.00'),
                'price': Decimal('99.00'),
                'max_daily_loss': Decimal('2500.00'),  # 5%
                'max_total_loss': Decimal('5000.00'),  # 10%
                'profit_target': Decimal('5000.00'),  # 10%
                'min_trading_days': 5,
                'max_position_size': Decimal('100.00'),
                'profit_split': Decimal('80.00'),
            },
            {
                'name': '$100K Challenge',
                'description': 'Trade with a $100,000 account and earn up to 80% profit split',
                'plan_type': 'EVALUATION',
                'starting_balance': Decimal('100000.00'),
                'price': Decimal('149.00'),
                'max_daily_loss': Decimal('5000.00'),  # 5%
                'max_total_loss': Decimal('10000.00'),  # 10%
                'profit_target': Decimal('10000.00'),  # 10%
                'min_trading_days': 5,
                'max_position_size': Decimal('100.00'),
                'profit_split': Decimal('80.00'),
            },
            {
                'name': '$200K Pro Challenge',
                'description': 'For experienced traders. $200,000 account with professional conditions.',
                'plan_type': 'EVALUATION',
                'starting_balance': Decimal('200000.00'),
                'price': Decimal('249.00'),
                'max_daily_loss': Decimal('10000.00'),  # 5%
                'max_total_loss': Decimal('20000.00'),  # 10%
                'profit_target': Decimal('20000.00'),  # 10%
                'min_trading_days': 7,
                'max_position_size': Decimal('100.00'),
                'profit_split': Decimal('85.00'),  # Higher split for larger accounts
            },
        ]
        
        created_count = 0
        for plan_data in plans:
            plan, created = PropFirmPlan.objects.get_or_create(
                name=plan_data['name'],
                defaults=plan_data
            )
            
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f'✓ Created plan: {plan.name}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'- Plan already exists: {plan.name}')
                )
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\n✓ Setup complete! Created {created_count} new plans.'
            )
        )
        self.stdout.write('\nYou can now:')
        self.stdout.write('1. Run the development server: python manage.py runserver')
        self.stdout.write('2. Access plans at: http://localhost:8000/api/prop-firm/plans/')
        self.stdout.write('3. Access Django admin at: http://localhost:8000/admin/')