# Pembayaran Termin manual entries

Apply `sql/20261125_add_manual_entry_installments.sql` before deploying. It adds the expense category,
widens manual-entry amounts to `NUMERIC(18, 2)`, and adds schedule metadata.

Select **Pembayaran Termin** from `GET /expense_categories`, then submit the
usual `POST /manual-entries/` payload with `entry_type: "expense"`,
`category_id` set to that category's ID, the **total** `amount`, and an integer
`installment_count` (1–600). The backend automatically selects the current
closing period, so `start_date` and `end_date` are not required; if supplied,
they are ignored for this category. "Current" uses the server's local date.

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

The response has `installment_group_id`, `installment_count`, and an `entries`
array containing each created manual entry. Existing listing, totals, and
reports see those entries individually. For a `26-25` outlet on 25 November,
the example starts with 26 October–25 November, then 26 November–25 December,
and so on. Missing or invalid closing settings use calendar months. If today
falls outside a configured non-contiguous closing range, the request is rejected. Days that do not exist
in a month are clamped to the month's last day.

The total is divided in cents, with remainder cents assigned to the earliest
installments. All rows are committed together. Installments cannot be edited
individually: deleting any row deletes the entire schedule, which can then be
recreated. Ordinary manual entries retain their existing API behavior.
