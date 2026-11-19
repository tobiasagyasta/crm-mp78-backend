# CRM MP78 Backend

## BI daily platform totals

`GET /bi/daily-totals` reads `daily_merchant_totals` on an inclusive transaction
date basis. `start_date` and `end_date` are required (`YYYY-MM-DD`); `brand` is
an optional exact, case-sensitive match to the outlet's current brand. Repeat
`platform` to filter: `gojek`, `grab`, or `shopee` (ShopeeFood). With no platform
filter, all three are included; ShopeePay is not included. Both active and
inactive outlets are included. Missing outlet/brand mappings appear with
`brand: null` unless a specific brand is selected.

```text
GET /bi/daily-totals?start_date=2026-01-01&end_date=2026-01-31&brand=MP78&platform=gojek&platform=grab&include_outlets=true&page=1&per_page=25
```

The response contains `filters`, overall `totals`, `brands` (including platform
breakdowns), and `daily` rows by brand, date and platform. Set
`include_outlets=true` to include range totals and platform breakdowns for each
outlet, with `outlets.pagination`. Pagination defaults to page 1 and 25 outlets;
`per_page` is capped at 100. Outlets are ordered case-insensitively by their
Gojek name, falling back to Grab, Webshop, Qpon, then outlet code; outlet code
breaks ties. Pagination does not affect the overall, brand or daily totals.
Amounts are decimal strings; absent combinations are omitted rather than filled
with zero. These are platform report totals, not bank mutation totals.

## Daily platform totals rebuild

**Deployment:** Apply `sql/20261120_create_monthly_totals.sql` to the target
database before deploying this version or running the daily worker. Back up
the database first.

The existing report importers immediately recalculate affected outlet/day totals,
including historical imports. For an additional daily repair pass, configure an
external scheduler (for example a Railway cron job) to run:

```bash
python -m app.workers.daily_totals
```

It rebuilds the last 30 completed calendar days in Asia/Jakarta for Gojek,
Grab and ShopeeFood, excluding ShopeePay. It needs the same database and app
environment as the backend. The command is a one-shot job: `worker.py` handles
the imports queue and does not schedule it. A failed rebuild exits with an error
and rolls back; configure scheduler monitoring and retries.

For a historical backfill or correction, use an inclusive date range:

```bash
python -m app.workers.daily_totals --start-date 2026-01-01 --end-date 2026-01-31
```

Use `--platform gojek` (repeatable) to restrict a rebuild, or
`--lookback-days 60` to change the default rolling window. The rebuild updates
existing rows in place, retaining zero-valued rows when source records have
been removed because saved matches can reference them. Old corrections outside
the rolling window require a manual range rebuild unless made through an
importer that already updates the affected partitions. This job does not
recompute mutation matches or aggregate unmatched mutations.

Progress is printed live to stderr; the final JSON summary remains on stdout.
Progress counts reflect processed partitions, not committed changes: the entire
rebuild is committed only after all selected platforms finish.

The daily worker also refreshes `monthly_totals` from daily totals for every
calendar month touched by its date range. Monthly rows are per outlet, month
(first day), and platform; `brand` comes from the outlet's current brand and
may be null for missing outlets or brands. They contain platform gross and net,
not mutation totals. An import refreshes daily totals immediately, but monthly
totals are refreshed on the next daily worker run if their month intersects the
worker's lookback. Imports in older months require a manual monthly rebuild:

```bash
python -m app.workers.monthly_totals --start-date 2026-01-01 --end-date 2026-12-31
```

This command uses existing daily totals; backfill daily totals first. It rebuilds
entire calendar months even if the given dates are mid-month. Add `--platform
shopee` to restrict it. Like the daily command it is transactional, idempotent,
and prints progress to stderr.

## Grab disbursement imports

Apply `sql/20261118_create_grab_disbursements.sql` before deploying the importer.
Upload CSV files as multipart form field `file` to
`POST /import-jobs/upload/grab-disbursement`; this uses the existing background
import-job endpoints for status and row issues. The disbursement ID is unique
across uploads. `Tanggal transfer` is stored as the transfer date; `Tanggal`,
bank name and bank statement code are not stored. The three- or four-digit
`Akun bank` value is matched against `rekenings.rekening_number` suffixes.
When several accounts match, the latest dated Grab bank mutation with the
store ID as platform code identifies the account. If the latest date has
conflicting accounts, the row is skipped as ambiguous. If no such history
exists, the outlet's linked rekening breaks the tie: the outlet is identified
by Grab store ID, or by Grab store name if the ID is not found. Unmatched and
unresolved rows are skipped with an import-job issue. Amounts are
expected as plain numeric values, as in Grab's CSV example.

The daily sheet and its preview show completed disbursements next to Grab Net.
They are matched by Grab store ID and summed on the transaction-date row for
the following calendar day's transfer date (H+1). A blank means no matching
completed disbursement was found; these are not bank-statement mutations.
Apply `sql/20261120_grab_disbursement_mutation_link.sql` before using
disbursement-based matching. The adjacent Grab Mutation column shows bank
credits on the same account and transfer date, matching the closest amount
within Rp5. One-to-one
matches are saved during the disbursement import when bank mutations
are already available, or when the sheet or preview is generated otherwise: the
mutation's platform code becomes the Grab store ID, and the payout stores the
mutation ID. Already coded mutations are left unchanged. Identical candidates
are paired in ID order; if different stores have identical payouts, this
assignment is arbitrary because the bank data cannot distinguish them.
If only some payouts match, the column contains only their matched credits.
`GRAB_DISBURSEMENT_FIRST_MATCHING` in
`app/services/excel_export/data_service.py` is currently `False`: the legacy
Grab Net-based matcher is allowed as a fallback on dates
without a completed disbursement. Previously coded Grab mutations are excluded
from that fallback. Set it to `True` to disable the fallback. The Grab
Pencairan column remains available in both modes.

🚀 A Flask-based REST API for authentication and user management in a CRM system.

## Features

- 🔐 User authentication (Register & Login)
- 🔑 JWT-based authentication
- 📄 Protected routes
- 🗄 PostgreSQL database integration with SQLAlchemy
- 📦 Modular MVC structure

## Setup & Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-username/crm-mp78-backend.git
   cd crm-mp78-backend
   ```

2. **Create and Activate Virtual Environment**

   **Mac/Linux:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

   **Windows:**

   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables**

   Create a `.env` file in the root directory and add:

   ```ini
   DATABASE_URL=postgresql://postgres:password@localhost:5432/crm_mp78_db
   JWT_SECRET_KEY=your-secret-key
   ```

5. **Initialize the Database**

   ```bash
   flask db init
   flask db migrate -m "Initial migration"
   flask db upgrade
   ```

6. **Run the Flask App**
   ```bash
   python -m app
   ```
   or (if using Flask CLI):
   ```bash
   flask run
   ```

## API Endpoints

| Method | Endpoint       | Description       | Auth Required |
| ------ | -------------- | ----------------- | ------------- |
| GET    | /              | Welcome message   | ❌ No         |
| POST   | /auth/register | User Registration | ❌ No         |
| POST   | /auth/login    | User Login (JWT)  | ❌ No         |
| GET    | /protected     | Protected Route   | ✅ Yes        |

## Project Structure

```bash
crm-mp78-backend/
│── app/
│   ├── __init__.py
│   ├── config/ # Configuration settings
│   ├── controllers/ # API route handlers
│   ├── models/ # Database models
│   ├── extensions/ # Extensions (DB, JWT)
│── migrations/ # Alembic migration files
│── .env # Environment variables
│── .gitignore
│── requirements.txt
│── README.md
```
