# CRM MP78 Backend

## TikTok disbursement imports

Apply `sql/20261125_create_tiktok_disbursements.sql` before deploying. Upload a
TikTok earnings CSV like `docs/earningsreport.csv` as multipart field `file` to
`POST /import-jobs/upload/tiktok-disbursement`. The endpoint returns a background
import job; use the existing import-job status and issue endpoints to track it.
The importer preserves IDs as text and stores amounts as decimals. Duplicate
statement number, item order ID and type combinations are skipped; the statement
number is also retained to distinguish entries from different statements.
Dates use the sample's `MM/DD/YYYY` format, with time on order creation.
The Daily sheet and its preview show `Tiktok Pencairan` next to `Tiktok Net`:
the sum of matching disbursement settlement amounts, attributed to the TikTok
report's order date. A day without matching entries is blank. All matching
statement/type entries, including refunds, are included. Item IDs are looked up
using the index on `tiktok_disbursements.item_order_id` created by the SQL file;
the existing trimmed TikTok report index is not needed for this lookup.

## Webshop and Maxim imports

Apply `sql/20261122_create_maxim_reports.sql` to the target database before
deploying this version. `POST /reports/upload/webshop` accepts the shared CSV export and
returns the existing import counts plus `platform_counts` for `webshop` and
`maxim`. Only delivered orders are imported. `Provider=klikit` with
`Order Source=Webshop` is Webshop; `Provider=Maxim` with an empty or Maxim
order source is Maxim. Other combinations are skipped and reported. Both use
the Webshop outlet-name mapping.

Maxim has no commission for now: its net is the CSV's Net Order Value.
Webshop retains its existing 3% import calculation. Both models retain the
historical field naming where `gross_value` stores Net Order Value. Maxim is
reported separately and is not currently part of the BI daily/monthly aggregate
endpoints described below.

Previously imported Maxim rows may already be in `webshop_reports`. They cannot
be identified from that table alone. Uploading them again is rejected as a
duplicate; reconcile them against original exports before moving any data.

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

### Monthly platform and daily mutation endpoints

`GET /bi/monthly-totals` accepts the same filters and optional outlet pagination
as the daily endpoint. The range must cover **whole calendar months**: start on
the first day and end on the last day (for the current month, use its calendar
end even if it is in the future). The `monthly` array has one row per brand,
month and platform; `month` is the first day of the calendar month. It reads
`monthly_totals`, whose brand and values reflect the last monthly refresh.
`totals` cover the full filtered range, independent of outlet pagination.

`GET /bi/daily-mutation-totals` accepts the same date, brand, platform and
pagination parameters. Its `daily` rows include transaction `date`, actual
`bank_date`, platform, brand, `attribution`, `amount`, `mutation_count` and
`missing_amount_count`. `totals_by_attribution` keeps `manual`, `sid_h1`,
`disbursement` and `unattributed` separate. `sid_h1` is heuristic; for
`unattributed` the date is only a candidate (`date_is_candidate: true`).
An outlet breakdown, when requested, excludes unattributed rows and includes
platform/attribution details. Amounts are decimal strings.

Without a brand filter, unattributed mutations appear in `daily` and its
attribution totals. With a brand filter, they instead appear in
`unattributed_all_brands`: they cannot be assigned to the selected brand and
must not be added to that brand's totals. They remain subject to the date and
platform filters. Monthly platform rows can be compared with mutation rows by
requesting the same whole-month range and grouping mutation transaction/candidate
dates by month, while keeping attribution categories separate. These endpoints
read precomputed aggregates and do not initiate matching or refresh them.

```text
GET /bi/monthly-totals?start_date=2026-01-01&end_date=2026-01-31&brand=MP78
GET /bi/daily-mutation-totals?start_date=2026-01-01&end_date=2026-01-31&brand=MP78&include_outlets=true
```

## Daily platform totals rebuild

**Deployment:** Apply `sql/20261120_create_monthly_totals.sql` and
`sql/20261121_create_daily_mutation_totals.sql` to the target database before
deploying this version or running the daily worker. Back up the database first.

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
recompute mutation matches.

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

### Daily mutation totals

The daily worker also rebuilds `daily_mutation_totals` for bank dates one day
after its transaction-date window. Gojek, Grab, and ShopeeFood bank credits
(`CR`, blank, or unknown direction) are included; explicit debits, unknown
platforms and rows without a bank date are excluded. Each mutation is counted
once. Amounts missing from source data are counted separately.

For Gojek and ShopeeFood, the worker uses saved manual matches first; otherwise
it attributes unique H+1 store-code candidates to a daily outlet total. Gojek
uses the full store ID and ShopeeFood uses its last four or five digits.
Each mutation must identify exactly one outlet on the previous day; multiple
mutations may be attributed to that same outlet. Codes that identify multiple
outlets remain unattributed. These SID matches are labelled `sid_h1` and are heuristic, not
confirmed. For Grab, only completed disbursement links are used; saved
amount-based Grab matches are ignored. Attributes are labelled `manual`,
`sid_h1`, or `disbursement` to distinguish their source.

All other credits are `unattributed`,
with no brand or outlet; their transaction date is only an H+1 candidate
(`bank_date - 1`), not a confirmed transaction date. This worker does **not**
write or update saved match records. Links
whose bank date is outside the lookback are not refreshed; backfill those dates
after older imports or match changes. Bank dates and attribution are retained
separately to avoid confusing candidates with actual matches.

The matching endpoints also refresh the selected platform and date range when
they save match results. Creating new Grab disbursement links refreshes their
affected bank dates in the same transaction, including when links are made
during a disbursement import or sheet generation. A refresh failure prevents
those matching changes from being committed. Direct database edits and other
changes that do not go through these paths still require a worker run or manual
backfill.

Backfill by candidate transaction date, without rebuilding platform totals:

```bash
python -m app.workers.mutation_totals --start-date 2026-01-01 --end-date 2026-01-31
```

The dates above process bank dates January 2 through February 1. Add
`--platform shopee` to restrict the rebuild. Mutation totals are separate from
`daily_merchant_totals` and are not yet included in the BI endpoint or monthly
totals. Mutation uploads alone do not immediately refresh this table.

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
`app/services/excel_export/data_service.py` is `True`: without a completed
disbursement, Grab Mutation stays blank. Across brands, Grab commissions and
closing and summary income use the completed payout when present and Grab Net
otherwise. This choice is made per day before summing the period. The daily Grab Net column
still shows the sales report amount for comparison; Grab Difference compares
the matched bank amount with the payout. For MPR brands, daily `GrabFood x OVO
(ac)` and closing deduct the GrabFood management commission from that day's
selected amount. OVO is exempt from that commission.

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
