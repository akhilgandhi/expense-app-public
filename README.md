# ExpenseFlow

**Event-Driven Personal Finance Management System**

A cloud-native microservices platform for tracking accounts, expenses, budgets, financial goals, and investments — built on Spring Boot 4, Kafka/RabbitMQ, and Kubernetes with Istio service mesh.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Microservices](#2-microservices)
3. [Quick Start](#3-quick-start)
4. [Interactive API Reference](#4-interactive-api-reference)
   - [Authentication](#41-authentication)
   - [Dashboard Service (Composite)](#42-dashboard-service-composite)
   - [Account Service](#43-account-service)
   - [Budget Service](#44-budget-service)
   - [Expense Service](#45-expense-service)
   - [Goal Service](#46-goal-service)
   - [Investment Service (GraphQL)](#47-investment-service-graphql)
5. [Event-Driven Design](#5-event-driven-design)
6. [Databases](#6-databases)
7. [Kubernetes & Istio Setup](#7-kubernetes--istio-setup)
8. [Observability](#8-observability)
9. [CI/CD Pipeline](#9-cicd-pipeline)
10. [Development Guide](#10-development-guide)
11. [Troubleshooting](#11-troubleshooting)
12. [Reference](#12-reference)

---

## 1. Architecture Overview

```
                         ┌──────────────────────────────────────────────────────┐
                         │                  Client / Browser                    │
                         └─────────────────────────┬────────────────────────────┘
                                                   │ HTTPS
                         ┌─────────────────────────▼────────────────────────────┐
                         │           Istio Ingress Gateway (minikube.me)        │
                         │              TLS termination  •  mTLS internal       │
                         └────────┬───────────────────────────────┬─────────────┘
                                  │                               │
                    ┌─────────────▼──────────┐      ┌────────────▼────────────┐
                    │   Authorization Server │      │   Dashboard Service      │
                    │   OAuth2 / JWT / OIDC  │      │   Composite Aggregator   │
                    │   Port: 9999           │      │   Port: 8089             │
                    │   Scope: account:read  │      │   OAuth2 Resource Server │
                    │         account:write  │      └──────────────────────────┘
                    └────────────────────────┘                 │
                                                               │ async messaging
                         ┌─────────────────────────────────────▼────────────────────────────┐
                         │                   Message Broker                                  │
                         │           RabbitMQ  (default)  │  Apache Kafka  (optional)        │
                         └──────┬────────────┬────────────┬────────────┬───────────┬─────────┘
                                │            │            │            │           │
                     ┌──────────▼──┐ ┌───────▼────┐ ┌───▼───────┐ ┌──▼──────┐ ┌─▼──────────┐
                     │   Account   │ │   Budget   │ │  Expense  │ │  Goal   │ │  Invest    │
                     │   Service   │ │   Service  │ │  Service  │ │ Service │ │  Service   │
                     │   Port 8081 │ │  Port 8082 │ │ Port 8083 │ │Port 8084│ │ Port 8082  │
                     │   MongoDB   │ │  MongoDB   │ │   MySQL   │ │  MySQL  │ │  MongoDB   │
                     │  account-db │ │  budget-db │ │ expensedb │ │ goaldb  │ │  invest-db │
                     └─────────────┘ └────────────┘ └───────────┘ └─────────┘ └────────────┘

Legend:
  ──► synchronous HTTP (via Dashboard → core services)
  ···► asynchronous messaging (event streams)
  All inter-service communication inside the mesh uses mTLS
```

### Key Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Event-Driven** | All state changes propagate as typed domain events over RabbitMQ/Kafka |
| **Choreography over Orchestration** | Services react to events; no central saga orchestrator |
| **Single Entrypoint** | Dashboard Service is the only externally exposed composite API |
| **Schema-First REST** | OpenAPI specs in `api-contracts/openapi/` → generated Java interfaces |
| **Schema-First Events** | AsyncAPI 3.0 specs in `api-contracts/events/` → generated Java payload classes via AsyncAPI CLI |
| **Resilient** | Circuit breaker + retry + time limiter on all upstream calls (Resilience4j) |
| **Observable** | Prometheus metrics, distributed tracing (Jaeger), structured logs (EFK) |
| **Secure** | OAuth2/JWT, mTLS between all services, Istio request authentication |

---

## 2. Microservices

| Service | Type | Port | Database | Messaging Role |
|---------|------|------|----------|----------------|
| [authorization-server](#authorization-server) | Auth | 9999 | In-memory | None |
| [dashboard-service](#42-dashboard-service-composite) | Composite | 8089 | None | Producer (5 topics) |
| [account-service](#43-account-service) | Core | 8081 | MongoDB | Consumer + event producer |
| [budget-service](#44-budget-service) | Core | 8082 | MongoDB | Consumer + event producer |
| [expense-service](#45-expense-service) | Core | 8083 | MySQL | Consumer + event producer |
| [goal-service](#46-goal-service) | Core | 8084 | MySQL | Consumer + event producer |
| [invest-service](#47-investment-service-graphql) | Core | 8082* | MongoDB | Consumer + event producer |
| gateway-server | Edge | 8443 | None | None (build only, no Helm chart yet) |

> \* invest-service port conflicts with budget-service in local config. Use Docker/K8s for concurrent local testing.

**Tech stack:** Java 25 · Spring Boot 4.0.2 · Spring Cloud 2025.1.1 · Gradle · Docker · Helm · Kubernetes · Istio

---

## 3. Quick Start

> **Prerequisites:** Minikube, kubectl, Helm ≥ 3.14, istioctl ≥ 1.23, Docker Desktop, Java 25, Node.js 18+, Jenkins (local brew), Nexus (local brew)

### 3.1 One-time cluster bootstrap

```bash
# 1. Start Minikube (10 GB RAM, 4 CPUs)
./scripts/setup-minikube.sh

# 2. Add host entry — required for all API links to work
echo "127.0.0.1 minikube.me" | sudo tee -a /etc/hosts

# 3. Keep tunnel open in a separate terminal (run this and leave it running)
sudo minikube tunnel --profile=expense-app

# 4. Start Nexus artifact server
brew services start nexus

# 5. Provision Nexus repositories (wait ~2 min for Nexus to boot first)
./scripts/setup-nexus.sh

# 6. Start Jenkins
brew services start jenkins-lts
```

### 3.2 Bootstrap infrastructure via Jenkins

```
Jenkins → infra → deploy-infra → Build with Parameters

ENVIRONMENT:      dev
MESSAGING_BROKER: rabbitmq        # or kafka
SETUP_CLUSTER:    ☐ unchecked     # check only to recreate Minikube from scratch
INSTALL_ISTIO:    ✓ checked
INSTALL_LOGGING:  ✓ checked
```

This deploys: cert-manager · Istio · EFK logging · MySQL · MongoDB · RabbitMQ · all K8s Secrets.
**Expected duration: 15–25 minutes on first run.**

### 3.3 Build and deploy services (in order)

```
1. Jenkins → api-contracts → main → Build Now          (shared library — build first; requires Node.js 18+)

2. Jenkins → authorization-server → main → Build Now
3. Jenkins → account-service → main → Build Now
4. Jenkins → expense-service → main → Build Now
5. Jenkins → goal-service → main → Build Now
6. Jenkins → budget-service → main → Build Now
7. Jenkins → invest-service → main → Build Now
8. Jenkins → dashboard-service → main → Build Now      (depends on all above)
```

Each build: compiles → tests → Jacoco (70% threshold) → Docker image → loads into Minikube → Helm deploy → smoke test.

### 3.4 Get your first access token

```bash
# writer token (read + write)
curl -s -X POST https://minikube.me/oauth2/token \
  -H "Authorization: Basic d3JpdGVyOnNlY3JldC13cml0ZXI=" \
  -d "grant_type=client_credentials&scope=account:read account:write" \
  -k | jq -r '.access_token'
```

Save the token — use it in the `Authorization: Bearer <token>` header for all API calls.

### 3.5 Create your first account and call the dashboard

```bash
TOKEN="<paste token here>"

# Create an account with a budget and expense
curl -s -X POST https://minikube.me/dashboard \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -k -d '{
    "accountId": 1,
    "name": "Primary Savings",
    "balance": 10000.00,
    "accountType": "SAVINGS",
    "budgets": [{
      "budgetId": "budget-001",
      "name": "Monthly Groceries",
      "totalAmount": 500.00,
      "startDate": "2026-05-01",
      "endDate": "2026-05-31",
      "categories": ["groceries"]
    }],
    "expenses": [],
    "goals": [],
    "investments": []
  }'

# Retrieve the full dashboard aggregate
curl -s https://minikube.me/dashboard/1 \
  -H "Authorization: Bearer $TOKEN" \
  -k | jq .
```

---

## 4. Interactive API Reference

### Environment URLs

Select your target environment. All API links in this document use these base URLs.

| Environment | Base URL | Swagger UI | Namespace |
|-------------|----------|------------|-----------|
| **Dev** (Minikube) | `https://minikube.me` | [Open Swagger UI ↗](https://minikube.me/openapi/swagger-ui.html) | `expense-app-dev` |
| **Prod** (Minikube) | `https://minikube.me` | [Open Swagger UI ↗](https://minikube.me/openapi/swagger-ui.html) | `expense-app` |
| **Local** | `https://localhost:8443` | [Open Swagger UI ↗](https://localhost:8443/openapi/swagger-ui.html) | — |

> **Note:** Dev and Prod share the same `minikube.me` ingress host. The active deployment (dev vs prod namespace) is controlled by which Helm releases are running. Ensure `sudo minikube tunnel --profile=expense-app` is running before using any Minikube URL.

---

### 4.1 Authentication

ExpenseFlow uses **OAuth2 Client Credentials** flow. All APIs (except the auth endpoints themselves) require a Bearer token.

**Authorization Server:** `https://minikube.me/oauth2/token` (dev/prod) · `https://localhost:8443/oauth2/token` (local)

#### Clients

| Client | Credentials (Base64) | Scopes | Use For |
|--------|---------------------|--------|---------|
| `writer` | `d3JpdGVyOnNlY3JldC13cml0ZXI=` | `account:read` + `account:write` | Create / Update / Delete |
| `reader` | `cmVhZGVyOnNlY3JldC1yZWFkZXI=` | `account:read` | Read-only queries |

> Decoded: `writer:secret-writer` · `reader:secret-reader`

#### Get Token — Writer (Dev/Prod)

```http
POST https://minikube.me/oauth2/token
Authorization: Basic d3JpdGVyOnNlY3JldC13cml0ZXI=
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&scope=account:read account:write
```

```bash
# curl equivalent
curl -s -X POST https://minikube.me/oauth2/token \
  -H "Authorization: Basic d3JpdGVyOnNlY3JldC13cml0ZXI=" \
  -d "grant_type=client_credentials&scope=account:read account:write" \
  -k | jq -r '.access_token'
```

#### Get Token — Reader (Dev/Prod)

```bash
curl -s -X POST https://minikube.me/oauth2/token \
  -H "Authorization: Basic cmVhZGVyOnNlY3JldC1yZWFkZXI=" \
  -d "grant_type=client_credentials&scope=account:read" \
  -k | jq -r '.access_token'
```

#### Using Swagger UI

1. Open [Swagger UI ↗](https://minikube.me/openapi/swagger-ui.html)
2. Click **Authorize** (lock icon)
3. Select `OAuth2` → enter client ID `writer` and secret `secret-writer`
4. Click **Authorize** — all requests in the UI will include the token automatically

---

### 4.2 Dashboard Service (Composite)

The Dashboard Service is the **primary API entry point**. It aggregates all core services and exposes a unified REST API. All write operations happen through here — the dashboard publishes events that each core service consumes independently.

**Base path:** `/dashboard`  
**Swagger UI:** [https://minikube.me/openapi/swagger-ui.html ↗](https://minikube.me/openapi/swagger-ui.html)  
**Required scope:** `account:read` (GET) · `account:write` (POST / DELETE)

---

#### `GET /dashboard/{accountId}` — Get full account aggregate

Returns account details with all associated budgets, expenses, goals, and investment summary.

```bash
curl -s https://minikube.me/dashboard/1 \
  -H "Authorization: Bearer $TOKEN" \
  -k | jq .
```

**Optional query parameters for testing resilience:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `delay` | int (seconds) | Introduce artificial delay in the account service call |
| `faultPercent` | int (0–100) | Percentage chance of triggering an artificial fault |

```bash
# Test circuit breaker — 3s delay triggers timeout (2s limit)
curl -s "https://minikube.me/dashboard/1?delay=3" \
  -H "Authorization: Bearer $TOKEN" -k

# Test fault injection — 50% fault probability
curl -s "https://minikube.me/dashboard/1?faultPercent=50" \
  -H "Authorization: Bearer $TOKEN" -k
```

**Response schema:**

```json
{
  "accountId": 1,
  "name": "Primary Savings",
  "balance": 10000.00,
  "type": "SAVINGS",
  "summary": {
    "budgets": [
      {
        "budgetId": "budget-001",
        "name": "Monthly Groceries",
        "totalAmount": 500.00,
        "remainingAmount": 414.50,
        "status": "ACTIVE"
      }
    ],
    "expenses": [
      {
        "expenseId": "expense-001",
        "description": "Weekly groceries",
        "amount": 85.50,
        "category": "groceries",
        "status": "COMPLETED"
      }
    ],
    "goals": [],
    "investmentSummary": {
      "totalInvestmentAmount": 5000.00,
      "currentValue": 5350.00,
      "totalReturns": 350.00,
      "totalNumberOfInvestments": 2
    }
  }
}
```

---

#### `POST /dashboard` — Create account with all resources

Creates an account and optionally bootstraps budgets, expenses, goals, and investments in a single call. The dashboard publishes events to each core service asynchronously.

```bash
curl -s -X POST https://minikube.me/dashboard \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -k -d '{
    "accountId": 1,
    "name": "Primary Savings",
    "balance": 10000.00,
    "accountType": "SAVINGS",
    "budgets": [
      {
        "budgetId": "budget-001",
        "name": "Monthly Groceries",
        "totalAmount": 500.00,
        "startDate": "2026-05-01",
        "endDate": "2026-05-31",
        "categories": ["groceries", "food"]
      }
    ],
    "expenses": [
      {
        "expenseId": "expense-001",
        "description": "Weekly groceries",
        "amount": 85.50,
        "date": "2026-05-10",
        "category": "groceries",
        "status": "COMPLETED"
      }
    ],
    "goals": [
      {
        "goalId": "goal-001",
        "name": "Emergency Fund",
        "targetAmount": 20000.00,
        "currentAmount": 10000.00,
        "startDate": "2026-01-01",
        "endDate": "2026-12-31",
        "priority": "HIGH"
      }
    ],
    "investments": [
      {
        "productName": "NIFTY 50 Index Fund",
        "type": "EQUITY",
        "amount": 5000.00,
        "currentValue": 5350.00
      }
    ]
  }'
```

**AccountType values:** `SAVINGS` · `CHECKING` · `CREDIT_CARD` · `CASH`

---

#### `POST /dashboard/{accountId}/budget` — Add a budget

```bash
curl -s -X POST https://minikube.me/dashboard/1/budget \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -k -d '{
    "budgetId": "budget-002",
    "name": "Transport",
    "totalAmount": 200.00,
    "startDate": "2026-05-01",
    "endDate": "2026-05-31",
    "categories": ["transport", "fuel"]
  }'
```

---

#### `POST /dashboard/{accountId}/expense` — Record an expense

```bash
curl -s -X POST https://minikube.me/dashboard/1/expense \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -k -d '{
    "expenseId": "expense-002",
    "description": "Monthly Metro Card",
    "amount": 150.00,
    "date": "2026-05-05",
    "category": "transport",
    "status": "COMPLETED",
    "budgetId": "budget-002"
  }'
```

> When `budgetId` is provided, the expense triggers budget deduction via the `expenses` topic. If account balance is insufficient, the expense is rejected and an `EXPENSE_REJECTED` event is published on the `expenses` topic — triggering cancellation in expense-service and budget reversal in budget-service.

**ExpenseStatus values:** `PENDING` · `COMPLETED` · `CANCELLED`

---

#### `POST /dashboard/{accountId}/goal` — Add a financial goal

```bash
curl -s -X POST https://minikube.me/dashboard/1/goal \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -k -d '{
    "goalId": "goal-002",
    "name": "Home Down Payment",
    "targetAmount": 500000.00,
    "currentAmount": 50000.00,
    "startDate": "2026-01-01",
    "endDate": "2030-12-31",
    "priority": "HIGH",
    "status": "ACTIVE",
    "tags": ["real-estate", "long-term"]
  }'
```

**GoalStatus values:** `ACTIVE` · `ACHIEVED` · `EXPIRED` · `AT_RISK`  
**GoalPriority values:** `HIGH` · `MEDIUM` · `LOW`

---

#### `DELETE /dashboard/{accountId}` — Delete account and all resources

Cascades deletion to all core services via domain events.

```bash
curl -s -X DELETE https://minikube.me/dashboard/1 \
  -H "Authorization: Bearer $TOKEN" \
  -k
# Returns 204 No Content
```

---

### 4.3 Account Service

Core service managing account balances and lifecycle. Exposed only via Dashboard events in production — but accessible directly for debugging.

**Direct access (dev — port-forward):**
```bash
kubectl port-forward -n expense-app-dev svc/account-service 8081:80
```

| Method | Path | Description | Required Scope |
|--------|------|-------------|----------------|
| `GET` | `/accounts/{accountId}` | Get account by ID | `account:read` |
| `POST` | `/accounts` | Create account | `account:write` |
| `DELETE` | `/accounts/{accountId}` | Delete account | `account:write` |

**Account entity fields:**

| Field | Type | Values |
|-------|------|--------|
| `accountId` | int | Unique identifier |
| `name` | string | Account display name |
| `accountType` | enum | `SAVINGS` · `CHECKING` · `CREDIT_CARD` · `CASH` |
| `balance` | decimal | Current balance |
| `currency` | string | ISO 4217 (e.g. `USD`) |
| `accountStatus` | enum | `ACTIVE` · `PENDING` · `CLOSED` |
| `tags` | string[] | User-defined tags |
| `notes` | string | Free-text notes |

**DLQ Handling:** Failed messages are stored in MongoDB collection `dlq_analytics` (30-day TTL) and optionally forwarded to AWS SNS topic `dlq-processor`.

---

### 4.4 Budget Service

Tracks spending budgets per account. Reacts to expense events and publishes budget state-change events consumed by the goal service.

**Direct access (dev — port-forward):**
```bash
kubectl port-forward -n expense-app-dev svc/budget-service 8082:80
```

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/budgets?accountId={id}` | Get all budgets for account |
| `POST` | `/budgets` | Create budget |
| `DELETE` | `/budgets?accountId={id}` | Delete all budgets for account |

**Budget entity fields:**

| Field | Type | Values |
|-------|------|--------|
| `budgetId` | string | Unique identifier |
| `accountId` | int | Parent account |
| `name` | string | Budget name |
| `totalAmount` | decimal | Budget cap |
| `remainingAmount` | decimal | Computed: total - spent |
| `startDate` / `endDate` | date | Budget period |
| `categories` | string[] | Expense categories covered |
| `status` | enum | `ACTIVE` · `EXPIRED` · `COMPLETED` · `EXCEEDED` |

**Events published by Budget Service:**

| Event | Trigger | Consumer |
|-------|---------|----------|
| `BUDGET_UPDATED` | Expense deducted from budget | goal-service |
| `BUDGET_EXCEEDED` | remainingAmount < 0 | goal-service |
| `BUDGET_THRESHOLD_WARNING` | remainingAmount < 20% of total | (no consumer yet) |
| `BUDGET_REVERSAL` | Expense rejected — budget deduction reversed | (future consumers) |

---

### 4.5 Expense Service

Records individual expense transactions. Validates balance against the account service and publishes results back as events.

**Direct access (dev — port-forward):**
```bash
kubectl port-forward -n expense-app-dev svc/expense-service 8083:80
```

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/expenses?accountId={id}` | Get all expenses for account |
| `POST` | `/expenses` | Create expense |
| `DELETE` | `/expenses?accountId={id}` | Delete all expenses for account |

**Expense entity fields:**

| Field | Type | Values |
|-------|------|--------|
| `expenseId` | string | Unique per account |
| `accountId` | int | Parent account |
| `description` | string | Expense description |
| `amount` | decimal | Expense amount |
| `date` | date | `YYYY-MM-DD` |
| `category` | string | User-defined category |
| `status` | enum | `PENDING` · `COMPLETED` · `CANCELLED` |
| `budgetId` | string | Optional — links expense to a budget |
| `tags` | string[] | User-defined tags |

**Retry policy:** 3 attempts · 500ms initial backoff · 1000ms max · multiplier 2.0

---

### 4.6 Goal Service

Tracks financial goals and reacts to budget events to update goal risk status.

**Direct access (dev — port-forward):**
```bash
kubectl port-forward -n expense-app-dev svc/goal-service 8084:80
```

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/goals?accountId={id}` | Get all goals for account |
| `POST` | `/goals` | Create goal |
| `DELETE` | `/goals?accountId={id}` | Delete all goals for account |

**Goal entity fields:**

| Field | Type | Values |
|-------|------|--------|
| `goalId` | string | Unique identifier |
| `accountId` | int | Parent account |
| `name` | string | Goal name |
| `targetAmount` | decimal | Goal target |
| `currentAmount` | decimal | Current progress |
| `startDate` / `endDate` | date | Goal period |
| `status` | enum | `ACTIVE` · `ACHIEVED` · `EXPIRED` · `AT_RISK` |
| `priority` | enum | `HIGH` · `MEDIUM` · `LOW` |
| `tags` | string[] | User-defined tags |

---

### 4.7 Investment Service (GraphQL)

Tracks investment portfolio. Exposes a **GraphQL API** instead of REST.

**Direct access (dev — port-forward):**
```bash
kubectl port-forward -n expense-app-dev svc/invest-service 8082:80
```

**GraphQL endpoint:** `POST /graphql`  
**GraphiQL UI (interactive browser IDE):** [http://localhost:8082/graphiql](http://localhost:8082/graphiql) (after port-forward)

**InvestmentType values:** `EQUITY` · `BONDS`

#### Sample Queries

```graphql
# Get all investments for an account
query GetInvestments {
  investments(accountId: 1) {
    productName
    type
    amount
    currentValue
  }
}
```

#### Sample Mutations

```graphql
# Create equity investment
mutation CreateEquityInvestment {
  createInvestment(input: {
    accountId: 1
    productName: "NIFTY 50 Index Fund"
    type: EQUITY
    amount: 5000.00
    currentValue: 5350.00
  }) {
    productName
    type
    amount
    currentValue
  }
}
```

```graphql
# Create bond investment
mutation CreateBondInvestment {
  createInvestment(input: {
    accountId: 1
    productName: "Government Bond 2030"
    type: BONDS
    amount: 10000.00
    currentValue: 10250.00
  }) {
    productName
    type
    amount
  }
}
```

```graphql
# Delete all investments for account
mutation DeleteInvestments {
  deleteInvestments(accountId: 1)
}
```

---

## 5. Event-Driven Design

### 5.1 Message Broker

ExpenseFlow supports two brokers, selected at deploy time via `MESSAGING_BROKER` parameter.

| Broker | Default | Config Profile |
|--------|---------|----------------|
| **RabbitMQ** | Yes | `docker` (default) |
| **Apache Kafka** | Optional | `kafka` |

All stream bindings use **Spring Cloud Stream** — swapping brokers requires no code changes, only configuration.

### 5.2 Topic Map

**One topic per domain aggregate.** All events for a domain travel on one topic regardless of direction. The `Event.Type` field routes within the topic — consumers switch on event type and ignore cases they don't own.

| Topic | Producers | Consumers | Event Types |
|-------|-----------|-----------|-------------|
| `accounts` | dashboard-service, account-service | account-service | `CREATE_ACCOUNT` · `DELETE_ACCOUNT` · `BALANCE_UPDATED` |
| `budgets` | dashboard-service, budget-service | budget-service, goal-service | `CREATE_BUDGET` · `DELETE_BUDGETS` · `BUDGET_UPDATED` · `BUDGET_EXCEEDED` · `BUDGET_THRESHOLD_WARNING` · `BUDGET_REVERSAL` |
| `expenses` | dashboard-service, expense-service, account-service | expense-service, budget-service, account-service | `CREATE_EXPENSE` · `DELETE_EXPENSES` · `EXPENSE_CREATED` · `EXPENSE_REJECTED` |
| `goals` | dashboard-service, goal-service | goal-service | `CREATE_GOAL` · `DELETE_GOALS` · `GOAL_AT_RISK` · `GOAL_COMPLETED` |
| `investments` | dashboard-service, invest-service | invest-service | `CREATE_INVESTMENT` · `DELETE_INVESTMENTS` · `INVESTMENT_CREATED` |

### 5.3 Event Type Registry

All event types are defined in `Event.Type` in `api-contracts`. Types are domain-scoped — every event is self-describing regardless of which topic it arrived on.

```
# CRUD commands — dashboard-service → core services
CREATE_ACCOUNT       DELETE_ACCOUNT
CREATE_EXPENSE       DELETE_EXPENSES
CREATE_BUDGET        DELETE_BUDGETS
CREATE_GOAL          DELETE_GOALS
CREATE_INVESTMENT    DELETE_INVESTMENTS

# Expense lifecycle
EXPENSE_CREATED      # expense-service → budget-service, account-service
EXPENSE_REJECTED     # account-service → expense-service, budget-service

# Budget state changes
BUDGET_UPDATED
BUDGET_EXCEEDED
BUDGET_THRESHOLD_WARNING
BUDGET_REVERSAL      # budget-service → (future consumers)

# Account state changes
BALANCE_UPDATED      # account-service → (dashboard, analytics)

# Goal state changes
GOAL_AT_RISK
GOAL_COMPLETED

# Investment lifecycle
INVESTMENT_CREATED   # invest-service → (goal-service, analytics)
```

### 5.4 Event Envelope

Every event is wrapped in `Event<K, T>` from `api-contracts`. The envelope carries idempotency and observability metadata alongside the domain payload.

```java
public class Event<K, T> {
    String   eventId;        // UUID v4 — idempotency key
    Type     eventType;      // domain-scoped enum value
    K        key;            // partition key (accountId)
    T        data;           // typed domain payload
    Instant  occurredAt;     // wall-clock time of originating action
    String   source;         // originating service name
    int      schemaVersion;  // payload schema version (starts at 1)
}
```

**Payload schemas** are defined as AsyncAPI 3.0 specs in `api-contracts/events/` and generated into Java classes via the AsyncAPI CLI at build time:

| Schema file | Generated classes |
|-------------|------------------|
| `expense-events.asyncapi.yaml` | `ExpenseCreated`, `ExpenseRejected` |
| `budget-events.asyncapi.yaml` | `BudgetUpdated`, `BudgetReversal` |
| `account-events.asyncapi.yaml` | `AccountUpdate` (BalanceUpdated payload) |
| `goal-events.asyncapi.yaml` | `GoalAtRisk`, `GoalCompleted` |
| `investment-events.asyncapi.yaml` | `InvestmentCreated` |

### 5.5 Event Flow Diagrams

#### Create Expense Flow

```
Client
  │
  ▼ POST /dashboard/{id}/expense
Dashboard Service
  │
  ├──[expenses topic: CREATE_EXPENSE]──────────────────────────────────────────►
  │                                                                             │
  │                                                                      Expense Service
  │                                                                             │
  │                                                              balance check against
  │                                                              account balance
  │                                                                             │
  │◄──[expenses topic: EXPENSE_CREATED or EXPENSE_REJECTED]────────────────────┘
  │                                                                             │
  │                           Account Service ◄────────────────────────────────┘
  │                           EXPENSE_CREATED → deducts balance, publishes BALANCE_UPDATED
  │                           EXPENSE_REJECTED → publishes on expenses topic
  │
  │                           Budget Service ◄──────[expenses: EXPENSE_CREATED]
  │                           deducts remainingAmount
  │                           publishes BUDGET_UPDATED/EXCEEDED/WARNING on budgets topic
  │                           ◄────────────────[expenses: EXPENSE_REJECTED]
  │                           reverses deduction, publishes BUDGET_REVERSAL on budgets topic
  │
  │                           Goal Service ◄────────[budgets: BUDGET_EXCEEDED]
  │                           updates goal status to AT_RISK
  │                           publishes GOAL_AT_RISK on goals topic
```

#### Account Deletion Cascade

```
Client
  │
  ▼ DELETE /dashboard/{accountId}
Dashboard Service
  │
  ├──[accounts: DELETE_ACCOUNT]──────► Account Service    (deletes account record)
  ├──[budgets: DELETE_BUDGETS]───────► Budget Service     (deletes all budgets)
  ├──[expenses: DELETE_EXPENSES]─────► Expense Service    (deletes all expenses)
  ├──[goals: DELETE_GOALS]───────────► Goal Service       (deletes all goals)
  └──[investments: DELETE_INVESTMENTS]► Invest Service    (deletes all investments)
```

### 5.6 Resilience Patterns

All Dashboard Service calls to upstream services use:

| Pattern | Config |
|---------|--------|
| **Circuit Breaker** | Failure threshold: 50% · Sliding window: 5 calls · Open wait: 10s |
| **Retry** | Max attempts: 3 · Wait: 1s between retries |
| **Time Limiter** | Timeout: 2s (20s in prod values) |

Consumer retry policy (all services):

| Setting | Value |
|---------|-------|
| Max attempts | 3 |
| Initial backoff | 500ms |
| Max backoff | 1000ms |
| Multiplier | 2.0 |

### 5.7 Dead Letter Topics

Every consumer binding has a Dead Letter Topic (DLT) configured so a malformed or unprocessable event does not halt partition consumption.

**Naming convention:** `{spring.application.name}.{functionBeanName}.dlt`

| Service | Consumer bean | DLT name |
|---------|--------------|----------|
| expense-service | `messageProcessor` | `expense-service.messageProcessor.dlt` |
| budget-service | `messageProcessor` | `budget-service.messageProcessor.dlt` |
| budget-service | `expenseEventProcessor` | `budget-service.expenseEventProcessor.dlt` |
| budget-service | `expenseRejectionProcessor` | `budget-service.expenseRejectionProcessor.dlt` |
| account-service | `messageProcessor` | `account-service.messageProcessor.dlt` |
| account-service | `expenseEventProcessor` | `account-service.expenseEventProcessor.dlt` |
| goal-service | `messageProcessor` | `goal-service.messageProcessor.dlt` |
| goal-service | `budgetEventProcessor` | `goal-service.budgetEventProcessor.dlt` |
| invest-service | `messageProcessor` | `invest-service.messageProcessor.dlt` |

Failed messages retry 3× then land in the DLT without blocking the consumer.

### 5.8 Idempotency

Each consumer service deduplicates events by `eventId` (UUID in the `Event<K,T>` envelope) to prevent double-processing under Kafka at-least-once delivery.

| Service | DB | Idempotency store |
|---------|----|------------------|
| expense-service | MySQL | `processed_events` table (Flyway migration) |
| goal-service | MySQL | `processed_events` table (Flyway migration) |
| account-service | MongoDB | `processed_events` collection (`@Document`) |
| budget-service | MongoDB | `processed_events` collection (`@Document`) |
| invest-service | MongoDB | `processed_events` collection (`@Document`) |

Before processing any event: check `eventId` not already in store. Insert after successful commit. dashboard-service is publish-only — no idempotency needed.

### 5.9 Partitioning Strategy

Partition key = **account ID** for all domain events. Guarantees total ordering of events within an account across all topics. Set via `MessageBuilder.setHeader("partitionKey", accountId)`.

Dashboard Service supports Kafka partitioned streams via the `streaming_partitioned` Spring profile:

```yaml
SPRING_PROFILES_ACTIVE: docker,kafka,streaming_partitioned
```

---

## 6. Databases

| Service | Engine | Database | Collections / Tables | Notes |
|---------|--------|----------|----------------------|-------|
| account-service | MongoDB | `account-db` | `accounts`, `processed_events`, `dlq_analytics` | DLQ TTL: 30 days |
| budget-service | MongoDB | `budget-db` | `budgets`, `processed_events` | |
| invest-service | MongoDB | `invest-db` | `investments`, `processed_events` | |
| expense-service | MySQL | `expensedb` | `expenses`, `processed_events` | JPA/Hibernate + Flyway |
| goal-service | MySQL | `goaldb` | `goals`, `processed_events` | JPA/Hibernate + Flyway |
| authorization-server | In-Memory | — | `RegisteredClientRepository` | Resets on restart |

### Schema Highlights

**`expenses` table unique constraint:**
```sql
UNIQUE INDEX expenses_unique_idx (accountId, expenseId)
```

**`processed_events` table (MySQL — expense-service, goal-service):**
```sql
CREATE TABLE processed_events (
    event_id     VARCHAR(36) PRIMARY KEY,
    processed_at TIMESTAMP  NOT NULL DEFAULT NOW()
);
```

**`accounts` MongoDB index:**
```
{ accountId: 1 } — unique
```

**`processed_events` MongoDB collection (account-service, budget-service, invest-service):**
```
{ _id: eventId }  — unique (enforced by @Id)
```

### Credentials (per environment)

| Environment | MySQL User | MongoDB User |
|-------------|------------|--------------|
| Dev | `mysql-user-prod` | `mongodb-user-dev` |
| Prod | `mysql-user-prod` | `mongodb-user-prod` |

> Credentials are injected via Kubernetes Secrets — never hardcoded in service config.

---

## 7. Kubernetes & Istio Setup

### 7.1 Cluster Configuration

```
Platform:     Minikube (Docker driver)
Profile:      expense-app
K8s Version:  v1.32.0
Resources:    10 GB RAM · 4 CPUs · 30 GB disk
Port mapping: 8080:80 · 8443:443 · 30080:30080 · 30443:30443
```

### 7.2 Namespaces

| Namespace | Contents | Istio Sidecar |
|-----------|----------|---------------|
| `expense-app-dev` | Datastores + dev service releases | Enabled |
| `expense-app` | Production service releases | Enabled |
| `istio-system` | Istio control plane · Prometheus · Grafana · Kiali · Jaeger | — |
| `logging` | Elasticsearch · Fluentd · Kibana | — |
| `cert-manager` | TLS certificate operator | — |

### 7.3 Helm Chart Structure

```
kubernetes/helm/
├── common/                    ← Shared templates (deployment, service, ingress, configmap, HPA)
├── components/
│   ├── account-service/
│   │   ├── Chart.yaml
│   │   ├── values.yaml          ← base values
│   │   ├── values-rabbitmq.yaml ← RabbitMQ binding overrides
│   │   └── values-kafka.yaml    ← Kafka binding overrides
│   ├── authorization-server/
│   ├── budget-service/
│   ├── dashboard-service/
│   ├── expense-service/
│   ├── goal-service/
│   ├── invest-service/
│   ├── kafka/
│   ├── mongodb/
│   ├── mysql/
│   └── rabbitmq/
└── environments/
    ├── infra-only/             ← RabbitMQ + datastores (no services)
    ├── infra-only-kafka/       ← Kafka + datastores (no services)
    ├── dev-env/                ← Full dev deployment
    ├── dev-env-kafka/          ← Full dev deployment with Kafka
    ├── prod-env/               ← Full prod deployment (ECR images)
    ├── istio-system/           ← Gateway · VirtualServices · TLS certs
    └── logging/                ← EFK stack
```

### 7.4 Service Resource Profile

Each service is deployed with:

```yaml
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    memory: 512Mi

# Istio sidecar resource limits
sidecar.istio.io/proxyCPURequest:  "10m"
sidecar.istio.io/proxyCPULimit:   "100m"
```

### 7.5 Istio Service Mesh

**Ingress Gateway:** All external traffic enters via Istio IngressGateway on `minikube.me`

**Routing (VirtualService paths):**

| Path | Routes To |
|------|-----------|
| `/dashboard/**` | dashboard-service |
| `/openapi/**` | dashboard-service (Swagger UI) |
| `/oauth2/**` | authorization-server |
| `/login/**` | authorization-server |
| `/graphql/**` | invest-service |

**mTLS:** DestinationRules enforce mTLS between all services:
```
dashboard-service ↔ account-service     (mTLS)
dashboard-service ↔ budget-service      (mTLS)
dashboard-service ↔ expense-service     (mTLS)
dashboard-service ↔ goal-service        (mTLS)
dashboard-service ↔ invest-service      (mTLS)
authorization-server ↔ all              (mTLS)
```

**JWT Validation:** Istio `RequestAuthentication` validates Bearer tokens issued by the authorization server before requests reach service pods.

### 7.6 TLS Certificates

cert-manager manages TLS certificates with a self-signed cluster issuer:

```yaml
# Certificate covers:
- minikube.me
- *.minikube.me
```

---

## 8. Observability

### 8.1 Metrics (Prometheus + Grafana)

All services expose Prometheus metrics on port **4004** at `/actuator/prometheus`.

Prometheus scrape annotation on all pods:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port:   "4004"
prometheus.io/path:   "/actuator/prometheus"
```

```bash
# Open Grafana dashboard
istioctl dashboard grafana
```

Custom metric tag applied to all services:
```
management.metrics.tags.application = ${spring.application.name}
```

### 8.2 Distributed Tracing (Jaeger)

All services export traces to Jaeger via the Zipkin protocol:

```
Endpoint: http://jaeger-collector.istio-system:9411/api/v2/spans
```

```bash
# Open Jaeger UI
istioctl dashboard jaeger
```

### 8.3 Service Mesh Visualization (Kiali)

```bash
# Open Kiali — shows live traffic topology, health, mTLS status
istioctl dashboard kiali
```

Kiali URL (direct): `http://localhost:20001`

### 8.4 Log Aggregation (EFK)

```
Fluentd (DaemonSet) → Elasticsearch → Kibana
```

| Component | Access |
|-----------|--------|
| Kibana | `http://localhost:5601` (port-forward from `logging` namespace) |
| Elasticsearch | `http://localhost:9200` (port-forward) |

```bash
# Access Kibana
kubectl port-forward -n logging svc/kibana 5601:5601
open http://localhost:5601
```

### 8.5 Health Endpoints

All services expose Spring Boot Actuator health endpoints:

```bash
# Check service health (after port-forward)
curl http://localhost:<port>/actuator/health

# Health groups checked at startup:
# - readinessState
# - rabbit (or kafka) binders
# - mongo (MongoDB services)
# - db (MySQL services)
```

| Service | Health Deps |
|---------|------------|
| account-service | readinessState · RabbitMQ/Kafka · MongoDB |
| budget-service | readinessState · RabbitMQ/Kafka · MongoDB |
| expense-service | readinessState · RabbitMQ/Kafka · MySQL |
| goal-service | readinessState · RabbitMQ/Kafka · MySQL |
| invest-service | readinessState · RabbitMQ/Kafka · MongoDB |
| dashboard-service | readinessState · RabbitMQ/Kafka |

---

## 9. CI/CD Pipeline

### 9.1 Pipeline Architecture

```
GitHub Repo Push
       │
       ▼
Jenkins Multibranch Pipeline
       │
  ┌────▼────┐
  │  Build  │  ./gradlew clean build jacocoTestReport
  └────┬────┘
       │
  ┌────▼────┐
  │ Quality │  SonarQube analysis + gate (optional)
  └────┬────┘
       │
  ┌────▼────┐
  │  Docker │  docker build -t expense-app/<svc>:<git-sha> .
  └────┬────┘
       │
  ┌────▼──────────┐
  │ Manual Gate   │  "Deploy <svc>:<sha> to dev?" — Approve / Abort
  └────┬──────────┘
       │ (on approval — triggers infra/deploy-service)
  ┌────▼──────────────────────────────────────────┐
  │ deploy-service pipeline                        │
  │                                                │
  │  1. helm lint (validate chart + values)        │
  │  2. minikube image load (host → Minikube)      │
  │  3. helm upgrade --install                     │
  │  4. kubectl rollout status                     │
  │  5. curl /actuator/health (smoke test)         │
  └────────────────────────────────────────────────┘
```

> **api-contracts build note:** The `api-contracts` pipeline runs AsyncAPI CLI via `npx` to generate event payload classes from `api-contracts/events/*.asyncapi.yaml`. Node.js 18+ must be installed on the Jenkins host (`brew install node`). A Node.js guard at the start of the Build stage fails fast with a clear message if Node.js is missing.

### 9.2 Jenkins Jobs

| Job | Type | Repo | Purpose |
|-----|------|------|---------|
| `infra/deploy-infra` | Pipeline | expense-app-iac | Bootstrap cluster (run once) |
| `infra/deploy-service` | Multibranch | expense-app-iac | Deploy a service (triggered by app builds) |
| `infra/rollback-service` | Pipeline | expense-app-iac | Manual `helm rollback` |
| `api-contracts` | Multibranch | api-contracts | Build + publish shared library to Nexus |
| `account-service` | Multibranch | expense-app-account-service | Build → deploy |
| `authorization-server` | Multibranch | expense-app-authorization-server | Build → deploy |
| `budget-service` | Multibranch | expense-app-budget-service | Build → deploy |
| `dashboard-service` | Multibranch | expense-app-dashboard-service | Build → deploy |
| `expense-service` | Multibranch | expense-app-expense-service | Build → deploy |
| `gateway-server` | Multibranch | expense-app-gateway-server | Build only (no Helm chart yet) |
| `goal-service` | Multibranch | expense-app-goal-service | Build → deploy |
| `invest-service` | Multibranch | expense-app-invest-service | Build → deploy |

### 9.3 Deploy-Infra Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ENVIRONMENT` | `dev` | Target namespace (`dev` = expense-app-dev, `prod` = expense-app) |
| `MESSAGING_BROKER` | `rabbitmq` | `rabbitmq` or `kafka` |
| `SETUP_CLUSTER` | false | Recreate Minikube from scratch (**destroys all data**) |
| `INSTALL_ISTIO` | true | Install cert-manager + Istio |
| `INSTALL_LOGGING` | true | Install EFK stack |

### 9.4 Deploy-Service Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `SERVICE_NAME` | account-service, budget-service, … | Target service |
| `IMAGE_TAG` | git short SHA | Docker image tag to deploy |
| `ENVIRONMENT` | `dev` \| `prod` | Target namespace |
| `ACTION` | `DEPLOY` \| `ROLLBACK` | Action to perform |
| `MESSAGING_BROKER` | `rabbitmq` \| `kafka` | Message broker config |

### 9.5 Image Registry

| Environment | Registry | Pattern |
|-------------|----------|---------|
| Dev | Minikube local | `expense-app/<service>:<sha>` |
| Prod | AWS ECR | `676497716946.dkr.ecr.us-east-1.amazonaws.com/expense-app/<service>:latest` |

Dev images never push to a registry — they are loaded directly into Minikube containerd via `minikube image load`.

### 9.6 Artifact Management (Nexus)

**Nexus URL:** `http://localhost:8085`  
**Credentials:** `admin` / `admin123`

| Repository | Type | Purpose |
|------------|------|---------|
| `maven-central` | Proxy | Maven Central mirror |
| `spring-milestones` | Proxy | Spring milestone releases |
| `spring-releases` | Proxy | Spring stable releases |
| `maven-releases` | Hosted | Published api-contracts artifacts |
| `maven-snapshots` | Hosted | Snapshot artifacts |
| `maven-public` | Group | Aggregates all above |

**Published artifacts:**
```
com.akhil.microservices:api-contracts:1.1.0   (event payload classes + typed Event<K,T> envelope)
com.akhil.microservices:util:1.0.0
```

---

## 10. Development Guide

### 10.1 Build a Single Service

```bash
# Build all (skips tests for speed)
./scripts/build.sh --skip-tests

# Build one service
./scripts/build.sh --service=account-service

# Build with specific tag
./scripts/build.sh --service=account-service --build-number=abc1234
```

### 10.2 Deploy Manually (Without Jenkins)

```bash
# Load image + deploy via Helm
./scripts/deploy.sh --services=account-service

# Quick redeploy (skips some steps)
./scripts/quick-deploy.sh --service=dashboard-service
```

### 10.3 Messaging Profiles

Services support two Spring profiles for broker selection:

```bash
# Default — RabbitMQ
SPRING_PROFILES_ACTIVE=docker

# Kafka
SPRING_PROFILES_ACTIVE=docker,kafka
```

### 10.4 Using HTTP Client Files

Each service has a `.http` file with all request examples (JetBrains HTTP Client / VS Code REST Client):

```
account-service/docs/account-service.http
budget-service/docs/budget-service.http
expense-service/docs/expense-service.http
goal-service/docs/goal-service.http
invest-service/docs/invest-service.http
dashboard-service/docs/dashboard-service.http
```

**Workflow:**
1. Open the `.http` file in your IDE
2. Run the **"Get Access Token - writer"** request first — token is stored in `client.global`
3. All subsequent requests in the file automatically use the stored token

**Environment variables in `.http` files:**

| Variable | Local | Dev/Prod |
|----------|-------|----------|
| `baseUrl` | `https://localhost:8443` | `https://minikube.me` |
| `authUrl` | `https://localhost:8443` | `https://minikube.me` |

### 10.5 Verifying Events

```bash
# RabbitMQ Management UI
kubectl port-forward -n expense-app-dev svc/rabbitmq 15672:15672
open http://localhost:15672  # guest/guest

# Kafka topics (if using Kafka)
kubectl exec -n expense-app-dev -it kafka-0 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## 11. Troubleshooting

### Pod fails with `ErrImageNeverPull`

```bash
# Check what images are loaded in Minikube
minikube image ls --profile=expense-app | grep expense-app

# Load manually
minikube image load expense-app/<service>:<sha> --profile=expense-app
```

### Jenkins: "Item type does not support parameters"

Cause: `infra/deploy-service/main` was never run — the `parameters {}` block is not registered.

```
Fix: Jenkins → infra → deploy-service → Scan Multibranch Pipeline Now
     Then: Jenkins → infra → deploy-service → main → Build Now (let it fail — that's OK)
     Re-run the failing service build
```

### api-contracts build fails with "node: command not found"

Cause: Node.js not installed. The api-contracts build invokes AsyncAPI CLI via `npx` to generate event payload classes — Node.js 18+ is required.

```bash
brew install node
node --version   # must be >= 18
npx --version    # must be >= 9

# Then re-run the api-contracts Jenkins pipeline
```

### Nexus unreachable from Gradle

```bash
brew services info nexus
curl -s http://localhost:8085/service/rest/v1/status
# If not running:
brew services start nexus
# Wait 2 min, then:
./scripts/setup-nexus.sh
```

### Groovy secret interpolation warning

```groovy
// Wrong — Groovy interpolates the secret
sh """echo '${NEXUS_CREDS_PSW}' | ..."""

// Correct — shell resolves the env var (escaped $)
sh """echo "\${NEXUS_CREDS_PSW}" | ..."""
```

### Circuit breaker open — dashboard returns 503

```bash
# Check CB state via actuator
kubectl port-forward -n expense-app-dev svc/dashboard-service 8089:80
curl http://localhost:8089/actuator/health | jq '.components.circuitBreakers'

# Wait 10s for CB to half-open, then retry
# Or restart account-service to clear the fault
kubectl rollout restart deployment/account-service -n expense-app-dev
```

### Full cluster reset

```bash
./scripts/teardown.sh --delete-minikube
# Then re-bootstrap:
Jenkins → infra → deploy-infra → Build with Parameters (SETUP_CLUSTER=true)
```

---

## 12. Reference

### Port Summary

| Service | Local Port | K8s Port | Actuator Port |
|---------|-----------|----------|---------------|
| gateway-server | 8443 (HTTPS) | — | 8443 |
| authorization-server | 9999 | 80 | 4004 |
| dashboard-service | 8089 | 80 | 4004 |
| account-service | 8081 | 80 | 4004 |
| budget-service | 8082 | 80 | 4004 |
| expense-service | 8083 | 80 | 4004 |
| goal-service | 8084 | 80 | 4004 |
| invest-service | 8082* | 80 | 4004 |
| Nexus | 8085 | — | — |
| Jenkins | 8080 | — | — |
| Prometheus | 9090 | — | — |
| Grafana | (dynamic) | — | — |
| Kiali | 20001 | — | — |
| Jaeger | 16686 | — | — |
| Kibana | 5601 | — | — |
| RabbitMQ Management | 15672 | — | — |

### GitHub Repositories

| Component | Repository |
|-----------|------------|
| Infrastructure (Helm + Jenkins) | `github.com/akhilgandhi/expense-app-iac` |
| API Contracts | `github.com/akhilgandhi/api-contracts` |
| Account Service | `github.com/akhilgandhi/expense-app-account-service` |
| Authorization Server | `github.com/akhilgandhi/expense-app-authorization-server` |
| Budget Service | `github.com/akhilgandhi/expense-app-budget-service` |
| Dashboard Service | `github.com/akhilgandhi/expense-app-dashboard-service` |
| Expense Service | `github.com/akhilgandhi/expense-app-expense-service` |
| Gateway Server | `github.com/akhilgandhi/expense-app-gateway-server` |
| Goal Service | `github.com/akhilgandhi/expense-app-goal-service` |
| Invest Service | `github.com/akhilgandhi/expense-app-invest-service` |

### Credentials Summary

| Service | Username | Password | Where Used |
|---------|----------|----------|------------|
| Nexus | `admin` | `admin123` | Jenkins credentials `nexus-credentials` |
| Jenkins | (initial admin password) | see setup | Browser unlock |
| RabbitMQ (dev) | `rabbit-user-prod` | `rabbit-pwd-prod` | K8s Secret |
| MySQL (dev) | `mysql-user-prod` | `mysql-pwd-prod` | K8s Secret |
| MongoDB (dev) | `mongodb-user-dev` | `mongodb-pwd-dev` | K8s Secret |
| Auth Server | `u` | `p` | OAuth2 login form |
| OAuth2 writer | `writer` | `secret-writer` | API client credentials |
| OAuth2 reader | `reader` | `secret-reader` | API client credentials |

### Tech Stack Summary

| Layer | Technology | Version |
|-------|-----------|---------|
| Language | Java | 25 |
| Framework | Spring Boot | 4.0.2 |
| Cloud | Spring Cloud | 2025.1.1 |
| Messaging | Spring Cloud Stream | — |
| REST Docs | SpringDoc OpenAPI | — |
| Event Schema | AsyncAPI | 3.0 |
| GraphQL | Spring GraphQL | — |
| Mapping | MapStruct | 1.6.3 |
| Resilience | Resilience4j | via Spring Cloud |
| Tracing | Micrometer / Brave (Zipkin) | — |
| Build | Gradle | latest wrapper |
| Node.js (build only) | Node.js | 18+ |
| Containers | Docker | ≥ 24 |
| Orchestration | Kubernetes (Minikube) | 1.32.0 |
| Package Manager | Helm | ≥ 3.14 |
| Service Mesh | Istio | ≥ 1.23 |
| TLS | cert-manager | — |
| CI/CD | Jenkins LTS | — |
| Artifacts | Sonatype Nexus OSS | — |
| Code Quality | SonarQube | optional |
| MongoDB | MongoDB | — |
| MySQL | MySQL | — |
| Message Broker | RabbitMQ / Apache Kafka | — |
| Logging | Elasticsearch + Fluentd + Kibana | — |
| Metrics | Prometheus + Grafana | — |
| Tracing UI | Jaeger | — |
| Mesh UI | Kiali | — |
