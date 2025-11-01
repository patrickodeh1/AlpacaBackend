# Prop Trading Firm Platform

A comprehensive Django-based proprietary trading firm platform with Stripe integration, real-time market data from Alpaca, and automated rule enforcement.

## 🚀 Features

### Core Functionality
- **Multiple Account Tiers**: Offer various challenge sizes ($50K, $100K, etc.)
- **Stripe Payment Integration**: Secure payment processing for account purchases
- **Automated Account Creation**: Instant account setup after successful payment
- **Paper Trading**: Risk-free simulated trading with real market data
- **Real-time Market Data**: Powered by Alpaca API for accurate pricing
- **Rule Enforcement Engine**: Automatic monitoring and enforcement of trading rules
- **Multi-Stage Evaluation**: Progress from evaluation to funded accounts

### Trading Rules & Limits
- Daily loss limits
- Total drawdown limits
- Position size restrictions
- Minimum trading days requirements
- Profit targets for evaluation phases

### Account Management
- Account lifecycle tracking (Pending → Active → Passed/Failed)
- Real-time balance updates
- Violation tracking and logging
- Detailed activity audit logs
- Account statistics and analytics

### Payout System
- Automated profit calculations
- Configurable profit splits
- Payout request management
- Integration with Stripe for disbursements

## 📋 Prerequisites

- Python 3.10+
- SQLite (development) / PostgreSQL (production)
- Redis (optional, for Celery tasks)
- Stripe Account
- Alpaca API Account (paper trading)

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd alpaca-main/backend
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Configuration

Create a `.env` file in the project root:

```env
# Django Settings
DJANGO_SECRET_KEY=your-secret-key-here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1

# Database (SQLite for dev, PostgreSQL for production)
# DB_NAME=propfirm_db
# DB_USER=postgres
# DB_PASSWORD=your-password
# DB_HOST=localhost
# DB_PORT=5432

# Stripe Configuration
STRIPE_PUBLIC_KEY=pk_test_your_public_key
STRIPE_SECRET_KEY=sk_test_your_secret_key
STRIPE_WEBHOOK_SECRET=whsec_your_webhook_secret

# Alpaca API (Paper Trading)
APCA_API_KEY=your_alpaca_key
APCA_API_SECRET_KEY=your_alpaca_secret
APCA_API_BASE_URL=https://paper-api.alpaca.markets
APCA_DATA_BASE_URL=https://data.alpaca.markets

# Email Configuration (Optional)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password
DEFAULT_FROM_EMAIL=noreply@propfirm.com

# Redis (Optional - for Celery)
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

### 5. Database Setup

```bash
# Run migrations
python manage.py makemigrations
python manage.py migrate

# Create superuser
python manage.py createsuperuser

# Load initial data (optional)
python manage.py loaddata initial_plans.json
```

### 6. Create Initial Plans

```bash
python manage.py shell
```

```python
from prop_firm.models import PropFirmPlan
from decimal import Decimal

# Create $50K Challenge
PropFirmPlan.objects.create(
    name="$50K Challenge",
    description="Prove your trading skills with a $50,000 account",
    plan_type="EVALUATION",
    starting_balance=Decimal('50000.00'),
    price=Decimal('99.00'),
    max_daily_loss=Decimal('2500.00'),  # 5%
    max_total_loss=Decimal('5000.00'),  # 10%
    profit_target=Decimal('5000.00'),  # 10%
    min_trading_days=5,
    max_position_size=Decimal('100.00'),
    profit_split=Decimal('80.00'),
    is_active=True
)

# Create $100K Challenge
PropFirmPlan.objects.create(
    name="$100K Challenge",
    description="Trade with a $100,000 account",
    plan_type="EVALUATION",
    starting_balance=Decimal('100000.00'),
    price=Decimal('149.00'),
    max_daily_loss=Decimal('5000.00'),
    max_total_loss=Decimal('10000.00'),
    profit_target=Decimal('10000.00'),
    min_trading_days=5,
    max_position_size=Decimal('100.00'),
    profit_split=Decimal('80.00'),
    is_active=True
)
```

### 7. Run Development Server

```bash
python manage.py runserver
```

The API will be available at `http://localhost:8000`

## 📚 API Endpoints

### Authentication
```
POST   /api/account/register/          # Register new user
POST   /api/account/login/             # Login
POST   /api/account/refresh_token/     # Refresh JWT token
GET    /api/account/profile/           # Get user profile
```

### Prop Firm Plans
```
GET    /api/prop-firm/plans/           # List available plans
GET    /api/prop-firm/plans/{id}/      # Get plan details
```

### Prop Firm Accounts
```
GET    /api/prop-firm/accounts/        # List user's accounts
GET    /api/prop-firm/accounts/{id}/   # Get account details
POST   /api/prop-firm/accounts/{id}/refresh_balance/  # Update balance
GET    /api/prop-firm/accounts/{id}/activities/       # Account activities
GET    /api/prop-firm/accounts/{id}/violations/       # Rule violations
GET    /api/prop-firm/accounts/{id}/statistics/       # Account stats
```

### Checkout & Payments
```
POST   /api/prop-firm/checkout/create_session/  # Create Stripe checkout
POST   /api/prop-firm/checkout/verify_payment/  # Verify payment
POST   /api/prop-firm/webhook/stripe/           # Stripe webhook handler
```

### Paper Trading
```
GET    /api/paper-trading/trades/       # List trades
POST   /api/paper-trading/trades/       # Create trade
POST   /api/paper-trading/trades/{id}/close/    # Close trade
POST   /api/paper-trading/trades/{id}/cancel/   # Cancel trade
```

### Market Data
```
GET    /api/core/assets/               # Search assets
GET    /api/core/assets/{id}/candles_v2/  # Get price data
GET    /api/core/watchlists/           # Manage watchlists
```

## 🔧 Configuration

### Stripe Setup

1. **Create Stripe Account**: Sign up at https://stripe.com
2. **Get API Keys**: Dashboard → Developers → API Keys
3. **Set up Webhook**:
   - Dashboard → Developers → Webhooks
   - Add endpoint: `https://your-domain.com/api/prop-firm/webhook/stripe/`
   - Select events: `payment_intent.succeeded`, `payment_intent.payment_failed`, `checkout.session.completed`

### Alpaca Setup

1. **Create Account**: Sign up at https://alpaca.markets
2. **Generate Paper Trading Keys**: Dashboard → Paper Trading → Generate Keys
3. **Add to Environment**: Copy keys to `.env` file

## 🏗️ Project Structure

```
alpacabackend/
├── account/              # User authentication & management
├── core/                 # Market data, assets, candles
├── paper_trading/        # Paper trading functionality
├── prop_firm/           # NEW: Prop firm specific features
│   ├── models.py        # Account, Plan, Payout models
│   ├── serializers.py   # API serializers
│   ├── views.py         # API endpoints
│   ├── services/
│   │   ├── stripe_service.py    # Stripe integration
│   │   └── rule_engine.py       # Trading rules enforcement
│   └── admin.py         # Django admin configuration
├── manage.py
└── requirements.txt
```

## 🎯 Usage Flow

### For Users (Traders)

1. **Register Account**: Create user account
2. **Browse Plans**: View available challenge tiers
3. **Purchase Plan**: Complete payment via Stripe
4. **Account Activation**: Automatic activation after payment
5. **Start Trading**: Place paper trades with real-time data
6. **Monitor Progress**: Track P&L, rules, and statistics
7. **Pass Evaluation**: Meet profit targets and trading requirements
8. **Request Payout**: Submit payout requests for funded accounts

### For Administrators

1. **Create Plans**: Define challenge parameters via Django admin
2. **Monitor Accounts**: View all accounts and their status
3. **Review Violations**: Check rule violations
4. **Process Payouts**: Review and approve payout requests
5. **Analytics**: Track platform performance and user metrics

## 🔒 Security Considerations

- **JWT Authentication**: Secure API access
- **Stripe Webhook Verification**: Validate all payment webhooks
- **CORS Configuration**: Properly configure allowed origins
- **Environment Variables**: Never commit secrets to repository
- **SQL Injection Protection**: Django ORM protects against SQL injection
- **HTTPS**: Use HTTPS in production

## 🚀 Deployment

### For Production

1. **Switch to PostgreSQL**:
   ```python
   # In settings.py, uncomment PostgreSQL config
   DATABASES = {
       'default': {
           'ENGINE': 'django.db.backends.postgresql',
           'NAME': os.getenv('DB_NAME'),
           # ...
       }
   }
   ```

2. **Set DEBUG=False**:
   ```env
   DEBUG=False
   ```

3. **Configure Allowed Hosts**:
   ```env
   ALLOWED_HOSTS=yourdomain.com,www.yourdomain.com
   ```

4. **Collect Static Files**:
   ```bash
   python manage.py collectstatic
   ```

5. **Use Production Server**:
   - Gunicorn: `gunicorn alpacabackend.wsgi:application`
   - Nginx for static files and reverse proxy

6. **Set up SSL Certificate**: Use Let's Encrypt or your provider

## 🧪 Testing

```bash
# Run all tests
python manage.py test

# Run specific app tests
python manage.py test prop_firm

# Run with coverage
pytest --cov=prop_firm
```

## 📊 Monitoring

### Check Account Rules
```bash
python manage.py shell
```

```python
from prop_firm.models import PropFirmAccount
from prop_firm.services.rule_engine import RuleEngine

account = PropFirmAccount.objects.get(account_number='PA12345678')
engine = RuleEngine(account)
violations = engine.check_all_rules()
print(violations)
```

### Update All Account Balances
```python
from prop_firm.models import PropFirmAccount

for account in PropFirmAccount.objects.filter(status='ACTIVE'):
    account.update_balance()
    account.check_rules()
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
- Create an issue on GitHub
- Email: support@yourpropfirm.com

## 🙏 Acknowledgments

- Alpaca Markets for market data API
- Stripe for payment processing
- Django & DRF community

---

**Happy Trading! 📈**