# API Documentation - Prop Trading Firm

Base URL: `http://localhost:8000/api`

## 🔐 Authentication

All endpoints (except public ones) require JWT authentication.

### Register User
```http
POST /account/register/
Content-Type: application/json

{
  "email": "trader@example.com",
  "name": "John Trader",
  "password": "SecurePass123!",
  "password2": "SecurePass123!",
  "tc": true
}

Response 201:
{
  "token": {
    "refresh": "eyJ0eXAiOiJKV1...",
    "access": "eyJ0eXAiOiJKV1..."
  },
  "msg": "Registration successful"
}
```

### Login
```http
POST /account/login/
Content-Type: application/json

{
  "email": "trader@example.com",
  "password": "SecurePass123!"
}

Response 200:
{
  "token": {
    "refresh": "eyJ0eXAiOiJKV1...",
    "access": "eyJ0eXAiOiJKV1..."
  },
  "msg": "Login successful"
}
```

### Refresh Token
```http
POST /account/refresh_token/
Content-Type: application/json

{
  "refresh": "eyJ0eXAiOiJKV1..."
}

Response 200:
{
  "access": "eyJ0eXAiOiJKV1...",
  "refresh": "eyJ0eXAiOiJKV1..."
}
```

### Using JWT Token
Include in header for authenticated requests:
```http
Authorization: Bearer eyJ0eXAiOiJKV1...
```

---

## 💼 Prop Firm Plans

### List Plans
```http
GET /prop-firm/plans/

Response 200:
{
  "msg": "Plans retrieved successfully",
  "data": [
    {
      "id": 1,
      "name": "$50K Challenge",
      "description": "Prove your trading skills...",
      "plan_type": "EVALUATION",
      "starting_balance": "50000.00",
      "price": "99.00",
      "max_daily_loss": "2500.00",
      "max_total_loss": "5000.00",
      "profit_target": "5000.00",
      "min_trading_days": 5,
      "max_position_size": "100.00",
      "profit_split": "80.00",
      "is_active": true,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### Get Plan Details
```http
GET /prop-firm/plans/{id}/

Response 200:
{
  "msg": "Plan details retrieved",
  "data": {
    "id": 1,
    "name": "$50K Challenge",
    ...
  }
}
```

---

## 🏦 Prop Firm Accounts

### List My Accounts
```http
GET /prop-firm/accounts/
Authorization: Bearer {token}

Query Parameters:
- status: filter by status (PENDING, ACTIVE, PASSED, FAILED)

Response 200:
{
  "msg": "Accounts retrieved successfully",
  "data": [
    {
      "id": 1,
      "account_number": "PA12345678",
      "status": "ACTIVE",
      "stage": "EVALUATION",
      "plan_name": "$50K Challenge",
      "starting_balance": "50000.00",
      "current_balance": "52000.00",
      "profit_earned": "2000.00",
      "trading_days": 3,
      "created_at": "2024-01-01T00:00:00Z",
      "total_pnl": "2000.00",
      "pnl_percentage": "4.00"
    }
  ],
  "count": 1
}
```

### Get Account Details
```http
GET /prop-firm/accounts/{id}/
Authorization: Bearer {token}

Response 200:
{
  "msg": "Account details retrieved",
  "data": {
    "id": 1,
    "account_number": "PA12345678",
    "status": "ACTIVE",
    "stage": "EVALUATION",
    "plan_details": {
      "name": "$50K Challenge",
      "max_daily_loss": "2500.00",
      ...
    },
    "starting_balance": "50000.00",
    "current_balance": "52000.00",
    "high_water_mark": "52500.00",
    "daily_loss": "0.00",
    "total_loss": "0.00",
    "profit_earned": "2000.00",
    "trading_days": 3,
    "violations": [],
    "recent_activities": [...],
    "can_trade": true
  }
}
```

### Refresh Account Balance
```http
POST /prop-firm/accounts/{id}/refresh_balance/
Authorization: Bearer {token}

Response 200:
{
  "msg": "Balance updated",
  "data": {...},
  "violations": []
}
```

### Get Account Activities
```http
GET /prop-firm/accounts/{id}/activities/
Authorization: Bearer {token}

Response 200 (Paginated):
{
  "count": 25,
  "next": "http://localhost:8000/api/prop-firm/accounts/1/activities/?page=2",
  "previous": null,
  "results": [
    {
      "id": 1,
      "activity_type": "TRADE_CLOSED",
      "description": "Trade closed: AAPL LONG - P&L: +$150.00",
      "created_at": "2024-01-01T10:30:00Z"
    }
  ]
}
```

### Get Account Violations
```http
GET /prop-firm/accounts/{id}/violations/
Authorization: Bearer {token}

Response 200:
{
  "msg": "Violations retrieved",
  "data": [
    {
      "id": 1,
      "violation_type": "DAILY_LOSS",
      "description": "Daily loss limit exceeded: $2600.00 > $2500.00",
      "threshold_value": "2500.00",
      "actual_value": "2600.00",
      "created_at": "2024-01-01T16:00:00Z"
    }
  ]
}
```

### Get Account Statistics
```http
GET /prop-firm/accounts/{id}/statistics/
Authorization: Bearer {token}

Response 200:
{
  "msg": "Statistics retrieved",
  "data": {
    "total_trades": 50,
    "open_trades": 2,
    "closed_trades": 48,
    "winning_trades": 30,
    "losing_trades": 18,
    "win_rate": 62.5,
    "total_pnl": "2000.00",
    "profit_earned": "2000.00",
    "total_loss": "0.00",
    "trading_days": 8,
    "days_active": 10,
    "average_win": "150.00",
    "average_loss": "-75.00"
  }
}
```

---

## 💳 Checkout & Payments

### Create Checkout Session
```http
POST /prop-firm/checkout/create_session/
Authorization: Bearer {token}
Content-Type: application/json

{
  "plan_id": 1,
  "success_url": "http://localhost:3000/success",
  "cancel_url": "http://localhost:3000/cancel"
}

Response 201:
{
  "msg": "Checkout session created",
  "data": {
    "session_id": "cs_test_...",
    "session_url": "https://checkout.stripe.com/pay/cs_test_...",
    "account_id": 1,
    "account_number": "PA12345678"
  }
}
```

### Verify Payment
```http
POST /prop-firm/checkout/verify_payment/
Authorization: Bearer {token}
Content-Type: application/json

{
  "account_id": 1,
  "payment_intent_id": "pi_..."
}

Response 200:
{
  "msg": "Payment verified and account activated",
  "data": {
    "id": 1,
    "account_number": "PA12345678",
    "status": "ACTIVE",
    ...
  }
}
```

### Stripe Webhook (No Auth Required)
```http
POST /prop-firm/webhook/stripe/
Stripe-Signature: t=...,v1=...
Content-Type: application/json

{
  "type": "payment_intent.succeeded",
  "data": {...}
}

Response 200:
{
  "status": "success"
}
```

---

## 📊 Paper Trading

### List Trades
```http
GET /paper-trading/trades/
Authorization: Bearer {token}

Query Parameters:
- status: OPEN, CLOSED, CANCELLED
- asset: filter by asset ID

Response 200:
{
  "count": 10,
  "results": [
    {
      "id": 1,
      "asset": 1,
      "asset_symbol": "AAPL",
      "direction": "LONG",
      "quantity": "10.000000",
      "entry_price": "150.00",
      "entry_at": "2024-01-01T10:00:00Z",
      "status": "OPEN",
      "current_value": "1520.00",
      "unrealized_pl": "20.00"
    }
  ]
}
```

### Create Trade
```http
POST /paper-trading/trades/
Authorization: Bearer {token}
Content-Type: application/json

{
  "asset": 1,
  "direction": "LONG",
  "quantity": "10",
  "entry_price": "150.00",
  "stop_loss": "145.00",
  "take_profit": "155.00"
}

Response 201:
{
  "id": 1,
  "asset": 1,
  "asset_symbol": "AAPL",
  "direction": "LONG",
  "quantity": "10.000000",
  "entry_price": "150.00",
  "status": "OPEN",
  ...
}
```

### Close Trade
```http
POST /paper-trading/trades/{id}/close/
Authorization: Bearer {token}
Content-Type: application/json

{
  "exit_price": "155.00",
  "notes": "Hit profit target"
}

Response 200:
{
  "id": 1,
  "status": "CLOSED",
  "exit_price": "155.00",
  "realized_pl": "50.00",
  ...
}
```

### Cancel Trade
```http
POST /paper-trading/trades/{id}/cancel/
Authorization: Bearer {token}

Response 200:
{
  "id": 1,
  "status": "CANCELLED",
  ...
}
```

---

## 📈 Market Data

### Search Assets
```http
GET /core/assets/search/?q=AAPL
Authorization: Bearer {token}

Response 200:
{
  "msg": "Assets found",
  "data": [
    {
      "id": 1,
      "symbol": "AAPL",
      "name": "Apple Inc",
      "asset_class": "us_equity",
      "exchange": "NASDAQ",
      "tradable": true
    }
  ]
}
```

### Get Asset Price Data
```http
GET /core/assets/{id}/candles_v2/?tf=1&limit=100
Authorization: Bearer {token}

Query Parameters:
- tf: timeframe in minutes (1, 5, 15, 30, 60, 240, 1440)
- limit: number of candles (default 100)
- offset: pagination offset

Response 200:
{
  "results": [
    {
      "date": "2024-01-01T10:00:00Z",
      "open": "150.00",
      "high": "151.50",
      "low": "149.50",
      "close": "151.00",
      "volume": "1000000.00"
    }
  ],
  "count": 1000,
  "next": true,
  "previous": false
}
```

---

## 💰 Payouts (Funded Accounts Only)

### Request Payout
```http
POST /prop-firm/payouts/request_payout/
Authorization: Bearer {token}
Content-Type: application/json

{
  "account_id": 1,
  "payment_method": "BANK_TRANSFER",
  "payment_details": {
    "account_number": "123456789",
    "routing_number": "987654321",
    "account_holder": "John Trader"
  }
}

Response 201:
{
  "msg": "Payout requested successfully",
  "data": {
    "id": 1,
    "account": 1,
    "account_number": "PA12345678",
    "amount": "1600.00",
    "profit_earned": "2000.00",
    "profit_split": "80.00",
    "status": "PENDING",
    "requested_at": "2024-01-01T10:00:00Z"
  }
}
```

### List Payouts
```http
GET /prop-firm/payouts/
Authorization: Bearer {token}

Response 200:
{
  "msg": "Payouts retrieved",
  "data": [
    {
      "id": 1,
      "account_number": "PA12345678",
      "amount": "1600.00",
      "status": "COMPLETED",
      "requested_at": "2024-01-01T10:00:00Z",
      "completed_at": "2024-01-03T10:00:00Z"
    }
  ]
}
```

---

## ⚠️ Error Responses

### 400 Bad Request
```json
{
  "msg": "Validation error",
  "errors": {
    "email": ["This field is required"],
    "password": ["Password too short"]
  }
}
```

### 401 Unauthorized
```json
{
  "detail": "Authentication credentials were not provided."
}
```

### 403 Forbidden
```json
{
  "detail": "You do not have permission to perform this action."
}
```

### 404 Not Found
```json
{
  "detail": "Not found."
}
```

### 500 Server Error
```json
{
  "msg": "Internal server error",
  "error": "Error message..."
}
```

---

## 🧪 Testing with cURL

### Complete Flow Example

```bash
# 1. Register
curl -X POST http://localhost:8000/api/account/register/ \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","name":"Test","password":"Pass123!","password2":"Pass123!","tc":true}'

# 2. Login
TOKEN=$(curl -X POST http://localhost:8000/api/account/login/ \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Pass123!"}' \
  | jq -r '.token.access')

# 3. Get Plans
curl http://localhost:8000/api/prop-firm/plans/ \
  -H "Authorization: Bearer $TOKEN"

# 4. Create Checkout
curl -X POST http://localhost:8000/api/prop-firm/checkout/create_session/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"plan_id":1,"success_url":"http://localhost:3000/success","cancel_url":"http://localhost:3000/cancel"}'

# 5. Get My Accounts
curl http://localhost:8000/api/prop-firm/accounts/ \
  -H "Authorization: Bearer $TOKEN"
```

---

## 📚 Additional Resources

- **Stripe Testing Cards**: https://stripe.com/docs/testing
- **Alpaca Paper Trading**: https://alpaca.markets/docs/trading/paper-trading/
- **Django REST Framework**: https://www.django-rest-framework.org/

---

**Need Help?** Open an issue on GitHub or check the setup guide!