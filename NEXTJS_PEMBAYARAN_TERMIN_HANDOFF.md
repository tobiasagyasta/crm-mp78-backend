# Pembayaran Termin: frontend handoff

## Overview

Add installment support to the existing manual expense form. This feature uses
the **existing** manual-entry endpoints; there is no new dedicated route.
When an expense uses the **Pembayaran Termin** category, one submission creates
all monthly installments immediately as separate manual entries.

Before enabling this in the frontend, the backend must be deployed and
`sql/20261125_add_manual_entry_installments.sql` applied. The SQL creates the
category and required columns. Do not hardcode its ID; IDs can differ between
environments.

## Category and form

1. Fetch expense categories using `GET /expense_categories` (no trailing slash).
   Find the category whose `name` is exactly `Pembayaran Termin` and use its
   returned `id`. If it is missing, do not enable the feature; report that
   backend setup is required.
2. In the existing manual entry form, show the installment UI only when
   `entry_type` is `expense` and this category is selected.
3. Require the outlet, brand, **total** amount, and number of installments
   (`installment_count`). Description is optional.
4. Require a positive amount with at most two decimal places and an integer
   installment count from 1 to 600. The amount must provide at least 0.01 per
   installment. Send the raw numeric amount or a decimal string, **not** a
   formatted value such as `"25.000.000"`.
5. Hide/disable the date inputs for this category and explain that the backend
   automatically determines dates from the outlet's current closing period.
   `start_date` and `end_date` are not required and are ignored if submitted.
6. For other categories, retain the existing form and request shape. Do not send
   `installment_count` for ordinary entries.

Suggested labels: **Total pembayaran**, **Jumlah termin (bulan)**, and
**Estimasi per termin**. A preview can divide the amount by the count, but the
backend is authoritative: it distributes remainder cents to the earliest
installments. Show the saved schedule from the response, not only the preview.

## Create schedule

`POST /manual-entries/`

```json
{
  "outlet_code": "OUT1",
  "brand_name": "Brand",
  "entry_type": "expense",
  "category_id": 123,
  "amount": "25000000.00",
  "installment_count": 5,
  "description": "Equipment"
}
```

Here `category_id` is an example only. A successful request returns **201** with
a schedule object, unlike the ordinary manual-entry POST response:

```json
{
  "installment_group_id": "3ccedb4b-b7be-4bee-a9d8-55b4c4df0393",
  "installment_count": 5,
  "entries": [
    {
      "id": 101,
      "outlet_code": "OUT1",
      "entry_type": "expense",
      "amount": 5000000.0,
      "description": "Equipment",
      "start_date": "2026-10-26",
      "end_date": "2026-11-25",
      "category_id": 123,
      "category_name": "Pembayaran Termin",
      "installment_group_id": "3ccedb4b-b7be-4bee-a9d8-55b4c4df0393",
      "installment_number": 1,
      "installment_count": 5,
      "created_at": "2026-11-25 10:00:00",
      "updated_at": "2026-11-25 10:00:00"
    },
    {
      "id": 102,
      "outlet_code": "OUT1",
      "entry_type": "expense",
      "amount": 5000000.0,
      "description": "Equipment",
      "start_date": "2026-11-26",
      "end_date": "2026-12-25",
      "category_id": 123,
      "category_name": "Pembayaran Termin",
      "installment_group_id": "3ccedb4b-b7be-4bee-a9d8-55b4c4df0393",
      "installment_number": 2,
      "installment_count": 5,
      "created_at": "2026-11-25 10:00:00",
      "updated_at": "2026-11-25 10:00:00"
    }
  ]
}
```

The `entries` array above is abbreviated: a successful request for five
installments returns all five entries, in installment order. The first
`amount` and the following amounts are **per installment**, not the total.
All entries share a group ID and are committed together. There is currently
no idempotency key or duplicate-submission protection: disable the submit
button while the request is in flight and avoid blindly retrying if the
response is lost, since a retry can create a second schedule.

### Period and rounding behavior

- The first period is the outlet closing period containing the **server's
  current local date**; future periods follow the same closing settings.
- Example: on November 25 with closing `26-25`, the first installment is
  October 26–November 25; on November 26 it is November 26–December 25.
- No valid closing setting means calendar months: the first period starts on
  the first day of the current month and ends on its last day.
- Closing days that do not exist in a month are clamped to the last day.
  If today's date falls in a gap in a non-contiguous configured closing range,
  the backend rejects the request instead of falling back to a calendar month.
- The total is split to two decimal places; any remainder cents are given to
  the earliest installments. For example, 100.00 over 3 gives 33.34, 33.33,
  and 33.33.
- Periods and amounts are fixed at creation; changing the outlet closing
  setting later does not reschedule existing rows.

Do not derive the authoritative schedule solely from the browser's date or
timezone. Display the server-returned dates. If a preview is displayed before
submission, label it as an estimate.

## List and display

Use the existing `GET /manual-entries/` route with required `outlet_code` and,
as needed, `entry_type`, `category_id`, `start_date`, `end_date`, `page`, and
`per_page`. The response remains `{ data, pagination, totals }`.

Each schedule row is returned as a separate manual entry. These fields are
present on every entry:

```ts
type ManualEntry = {
  id: number;
  outlet_code: string;
  entry_type: "income" | "expense";
  amount: number;
  description: string | null;
  start_date: string; // YYYY-MM-DD
  end_date: string;   // YYYY-MM-DD
  category_id: number;
  category_name: string | null;
  installment_group_id: string | null;
  installment_number: number | null;
  installment_count: number | null;
  created_at: string; // YYYY-MM-DD HH:mm:ss
  updated_at: string;
};
```

`brand_name` is required on creation but is not included in the current entry
response. For ordinary entries the three installment fields are `null`. For a
schedule row, show e.g. **Termin 2/5**, its per-period amount, and its
`start_date`–`end_date`. Rows are ordered by start date descending, so later
installments appear first.

Date filters select entries whose periods **overlap** the requested range,
and totals sum the full amounts of matching rows, ignoring pagination; they
do not prorate partial overlaps. Without date filters, totals include future
installment rows too. Request the appropriate closing-period range for
period-specific totals.

There is no group-ID filter or dedicated schedule-fetch endpoint. Pagination
and date filters can hide other installments in the same group. Do not assume
the currently visible rows constitute the whole schedule, and do not derive a
schedule's original total by summing only the visible page.

## Editing and deletion

- If `installment_group_id` is not null, disable the normal individual edit
  action. `PUT /manual-entries/<entry_id>` returns 400 for any schedule row.
  Ordinary entries cannot be converted to this category with PUT either.
- `DELETE /manual-entries/<entry_id>` on **any** schedule row deletes the
  **entire group**, including past and future installments. Show an explicit
  confirmation warning before submitting:
  “Hapus seluruh jadwal termin? Semua termin, termasuk periode sebelumnya dan
  berikutnya, akan dihapus.”
- On success the response is `200` with
  `{ "message": "Installment schedule deleted successfully" }`. Refresh the
  list and totals. Deleting an ordinary entry keeps its existing single-row
  behavior.
- To change a schedule, the user must delete the whole schedule and create a
  new one. Warn that recreation starts from the **current** closing period and
  that delete and recreate are separate operations, not an atomic edit.

## Errors and acceptance checks

Display the backend's `{ "error": "..." }` message when available. Validation
failures return `400`, including missing fields, invalid amount/count, unknown
outlet, a date outside a configured closing range, and unsupported individual
edits. A missing entry returns `404`; unexpected failures may return `500`.

Before release, verify:

1. Category is fetched dynamically; the installment controls appear only for
   the special expense category and normal entries still work.
2. Rp25,000,000 over 5 creates five Rp5,000,000 entries and displays their
   returned dates; no dates are required in the request.
3. A `26-25` outlet switches to the next period on the 26th; a missing closing
   setting uses calendar-month boundaries.
4. A non-divisible amount displays the backend's exact rounding and sums to the
   submitted total.
5. The list shows `Termin n/N`, period filters and totals behave as described,
   individual edits are disabled, and deleting any row warns about and removes
   the full schedule.
6. Form validation covers a missing/invalid count, non-positive amount, and an
   amount too small to allocate at least 0.01 to every installment.
