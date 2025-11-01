# Prop Trading Firm - Complete Setup Guide

## 📋 Overview

This guide will walk you through setting up your prop trading firm platform from scratch. Follow each step carefully.

## 🎯 What You're Building

A fully-functional proprietary trading firm that:
- Accepts payments via Stripe for trading challenges
- Creates trading accounts automatically
- Enforces trading rules (daily loss, total loss, profit targets)
- Tracks trader performance
- Handles payouts for successful traders
- Provides real-time market data via Alpaca

## ⚡ Quick Start (5 Minutes)

### 1. Install Dependencies

```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and add your keys:
- Get Alpaca keys from: https://app.alpaca.markets/paper
- Get Stripe keys from: https://dashboard.stripe.com/test/apikeys

### 3. Initialize Database

```bash
python manage.py makemigrations
python manage.py migrate
python manage.py createsuperuser
python manage.py setup_propfirm
```

### 4. Start Server

```bash
python manage.py runserver
```

Visit: http://localhost:8000/admin

## 📦 Detailed Installation

### Prerequisites Check

```bash
# Check Python version (need 3.10+)
python --version

# Check pip
pip --version

# Check git
git --version
```

### Step-by-Step Setup

#### 1. **Clone & Navigate**

```bash
git clone <your-repo>
cd backend
```

#### 2. **Create Virtual Environment**

```bash
# Create venv
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Verify activation
which python  # Should point to venv
```

#### 3. **Install Python Packages**

```bash
# Upgrade pip first
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Verify installation
pip list
```

#### 4. **Configure Environment Variables**

```bash
# Copy template
cp .env.example .env

# Edit with your favorite editor
nano .env  # or code .env, vim .env, etc.
```

**Required Variables:**

```env
# Alpaca (REQUIRED)
APCA_API_KEY=PK...
APCA_API_SECRET_KEY=...

# Stripe (REQUIRED for payments)
STRIPE_PUBLIC_KEY=pk_test_...
STRIPE_SECRET_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...

# Django (Generate new secret)
DJANGO_SECRET_KEY=your-random-50-char-string
```

**Generate Django Secret Key:**

```python
python -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())'
```

#### 5. **Database Setup**

```bash
# Create migrations for all apps
python manage.py makemigrations account
python manage.py makemigrations core
python manage.py makemigrations paper_trading
python manage.py makemigrations prop_firm

# Apply migrations
python manage.py migrate

# Verify database
python manage.py showmigrations
```

#### 6. **Create Admin User**

```bash
python manage.py createsuperuser
```

Enter:
- Email: admin@example.com
- Name: Admin
- Password: (choose strong password)

#### 7. **Load Initial Data**

```bash
# Create prop firm plans
python manage.py setup_propfirm

# Optional: Sync Alpaca assets
python manage.py shell
```

In shell:
```python
from core.tasks import alpaca_sync_task
alpaca_sync_task.delay()
exit()
```

## 🔑 Getting API Keys

### Alpaca API Keys (Market Data)

1. **Sign Up**: Go to https://alpaca.markets
2. **Navigate**: Dashboard → Paper Trading
3. **Generate Keys**: Click "Generate New Keys"
4. **Copy**: Save API Key and Secret Key
5. **Add to .env**:
   ```env
   APCA_API_KEY=PKxxxxxxxxx
   APCA_API_SECRET_KEY=xxxxxxxxx
   ```

### Stripe API Keys (Payments)

1. **Sign Up**: Go to https://stripe.com
2. **Navigate**: Dashboard → Developers → API Keys
3. **Get Test Keys**: Copy publishable and secret keys
4. **Add to .env**:
   ```env
   STRIPE_PUBLIC_KEY=pk_test_xxxxx
   STRIPE_SECRET_KEY=sk_test_xxxxx
   ```

#### Setting up Stripe Webhook

1. **Navigate**: Dashboard → Developers → Webhooks
2. **Add Endpoint**: Click "Add endpoint"
3. **URL**: `http://localhost:8000/api/prop-firm/webhook/stripe/`
4. **Events**: Select:
   - `payment_intent.succeeded`
   - `payment_intent.payment_failed`
   - `checkout.session.completed`
5. **Get Secret**: Copy webhook signing secret
6. **Add to .env**:
   ```env
   STRIPE_WEBHOOK_SECRET=whsec_xxxxx
   ```

## 🧪 Testing the Installation

### 1. Start Server

```bash
python manage.py runserver
```

### 2. Test Admin Panel

Visit: http://localhost:8000/admin
Login with superuser credentials

### 3. Test API Endpoints

```bash
# Get plans
curl http://localhost:8000/api/prop-firm/plans/

# Register user
curl -X POST http://localhost:8000/api/account/register/ \
  -H "Content-Type: application/json" \
  -d '{
    "email": "trader@example.com",
    "name": "Test Trader",
    "password": "TestPass123!",
    "password2": "TestPass123!",
    "tc": true
  }'
```

### 4. Test Stripe Integration

```bash
# Use Stripe test card: 4242 4242 4242 4242
# Any future expiry date
# Any 3-digit CVC
```

## 🏗️ Project Structure

```
backend/
├── account/              # User authentication
├── core/                 # Market data & assets
├── paper_trading/        # Trading functionality
├── prop_firm/           # 🆕 Prop firm features
│   ├── models.py        # Account, Plan, Payout models
│   ├── views.py         # API endpoints
│   ├── serializers.py   # Data serialization
│   ├── admin.py         # Admin interface
│   ├── services/
│   │   ├── stripe_service.py    # Payment processing
│   │   └── rule_engine.py       # Rule enforcement
│   └── management/commands/
│       └── setup_propfirm.py    # Initial setup
├── alpacabackend/       # Django settings
├── manage.py
├── requirements.txt
├── .env.example
└── README.md
```

## 🎨 Customizing Your Prop Firm

### Creating Custom Plans

```bash
python manage.py shell
```

```python
from prop_firm.models import PropFirmPlan
from decimal import Decimal

PropFirmPlan.objects.create(
    name="Custom $150K Challenge",
    description="Your custom challenge",
    plan_type="EVALUATION",
    starting_balance=Decimal('150000.00'),
    price=Decimal('199.00'),
    max_daily_loss=Decimal('7500.00'),
    max_total_loss=Decimal('15000.00'),
    profit_target=Decimal('15000.00'),
    min_trading_days=5,
    max_position_size=Decimal('100.00'),
    profit_split=Decimal('80.00'),
    is_active=True
)
```

### Adjusting Trading Rules

Edit in Django Admin or shell:

```python
plan = PropFirmPlan.objects.get(name="$50K Challenge")
plan.max_daily_loss = Decimal('3000.00')  # Change to 6%
plan.min_trading_days = 7  # Require more days
plan.save()
```

## 🚀 Deploying to Production

### 1. Switch to PostgreSQL

Install PostgreSQL, then update `.env`:

```env
DB_NAME=propfirm_prod
DB_USER=propfirm_user
DB_PASSWORD=secure_password
DB_HOST=localhost
DB_PORT=5432
```

Update `settings.py` to use PostgreSQL config.

### 2. Security Settings

```env
DEBUG=False
ALLOWED_HOSTS=yourdomain.com,www.yourdomain.com
DJANGO_SECRET_KEY=generate-new-production-key
```

### 3. Collect Static Files

```bash
python manage.py collectstatic
```

### 4. Use Production Server

```bash
# Install gunicorn
pip install gunicorn

# Run
gunicorn alpacabackend.wsgi:application --bind 0.0.0.0:8000
```

### 5. Setup Nginx

```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /static/ {
        alias /path/to/staticfiles/;
    }

    location /media/ {
        alias /path/to/media/;
    }
}
```

## 🐛 Troubleshooting

### Database Issues

```bash
# Reset database (DEV ONLY!)
rm db.sqlite3
python manage.py migrate
python manage.py createsuperuser
python manage.py setup_propfirm
```

### Migration Issues

```bash
# Show migrations
python manage.py showmigrations

# Reset migrations (careful!)
find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
find . -path "*/migrations/*.pyc" -delete
python manage.py makemigrations
python manage.py migrate
```

### Import Errors

```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Stripe Webhook Not Working

1. Check webhook secret in `.env`
2. Verify endpoint URL is correct
3. Use Stripe CLI for local testing:
   ```bash
   stripe listen --forward-to localhost:8000/api/prop-firm/webhook/stripe/
   ```

## 📚 Next Steps

1. **Customize Plans**: Adjust account sizes and rules
2. **Add Email Notifications**: Configure SMTP for user emails
3. **Build Frontend**: Create user-facing trading dashboard
4. **Setup Monitoring**: Add error tracking (Sentry)
5. **Configure Backups**: Automated database backups
6. **Scale**: Add Redis for caching, Celery for background tasks

## 🆘 Getting Help

- Check logs: `tail -f logs/propfirm.log`
- Django shell: `python manage.py shell`
- Run tests: `python manage.py test prop_firm`

## 📞 Support

Issues? Questions?
- Create GitHub issue
- Check Django docs: https://docs.djangoproject.com
- Check DRF docs: https://www.django-rest-framework.org

---

**You're all set! Start building your prop trading firm! 🚀**