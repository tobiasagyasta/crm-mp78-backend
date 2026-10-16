# Import-job row logs: Next.js detail page handoff

Use this document in the **separate Next.js frontend repository** to add a "Skipped rows needing review" section to the existing `import-jobs/[jobId]/page.tsx` route. This repository contains the Flask backend, not the Next.js page. Keep the page's existing job status, progress, and error UI; add the issue details below the counters.

## Copy/paste task for a frontend coding agent

> Inspect the existing `import-jobs/[jobId]/page.tsx`, API client, authentication, and UI patterns. Add an issue section for non-duplicate skipped rows using the API contract below. Show the job's duplicate count separately, show reason-group totals, a paginated/filterable issue table with expandable source-row details, and a CSV download. Integrate with existing polling while the job is queued/processing; refresh once at terminal state. Handle legacy jobs, empty results, loading, errors, and multiple attempts. Use the existing base URL/auth/client mechanisms; run the available frontend lint/typecheck. Do not implement this from the old `skipped_rows_debug` RQ result.

## Backend contract (implemented)

All paths below are relative to the existing backend API base URL. Follow the frontend's current authenticated request conventions, if any.

### `GET /import-jobs/{jobId}`

The existing detail response still contains `status`, `total_rows`, `processed_rows`, `inserted_rows`, `skipped_rows`, `failed_rows`, `error_message`, and the job's other fields. It now additionally includes:

```json
{
  "id": 123,
  "status": "completed",
  "skipped_rows": 62,
  "duplicate_rows": 60,
  "non_duplicate_skipped_rows": 2,
  "attempt_number": 1,
  "issue_summary": [
    { "reason_code": "OUTLET_NOT_FOUND", "count": 1 },
    { "reason_code": "PARSE_ERROR", "count": 1 }
  ]
}
```

This is an excerpt, not the entire job object. `skipped_rows` **includes** duplicates and reviewable skips: do not add the three counters together. For older imports, `duplicate_rows` and `non_duplicate_skipped_rows` can be `null`, and `attempt_number` can be `0`; detailed logs were never recorded for those jobs. `issue_summary` is for the **current attempt only**, ordered by count descending. During an active import, the summary or issue list may be empty until the worker saves diagnostics; refresh after completion/failure.

### `GET /import-jobs/{jobId}/issues?page=1&per_page=50&reason_code=OUTLET_NOT_FOUND`

`reason_code` is optional (omit it for all reasons). `attempt_number` is optional and defaults to the job's latest attempt. `per_page` defaults to 50 and is capped at 100. Pages are 1-based. Entries are ordered by issue ID ascending.

```json
{
  "data": [
    {
      "id": 456,
      "row_number": 128,
      "sheet_name": null,
      "reason_code": "OUTLET_NOT_FOUND",
      "message": "No outlet matches Merchant ID or name",
      "field_name": null,
      "context": { "merchant_id": "unknown", "merchant_name": "Example" },
      "row_data": { "Merchant ID": "unknown", "Merchant name": "Example" },
      "created_at": "2026-10-16T12:34:56"
    }
  ],
  "attempt_number": 1,
  "filters": { "reason_code": "OUTLET_NOT_FOUND" },
  "pagination": {
    "current_page": 1,
    "per_page": 50,
    "total_pages": 1,
    "total_records": 1
  }
}
```

`row_data` can be a key/value object (Grab/Gojek/Shopee), an array (TikTok/mutation), or `null`. `context` can be an object or `null`. `sheet_name` is used for Excel mutation files. `row_number` identifies the source CSV/worksheet row. Fields in the source file can vary: render them dynamically rather than hardcoding column names. Treat values as untrusted text; do not render raw HTML. This endpoint contains **non-duplicate** skips only. The backend does not provide per-row duplicate entries.

If an explicitly requested attempt is less than 1 or greater than the job's `attempt_number`, the issues/export endpoints return HTTP 400. A missing job returns HTTP 404. Legacy/queued jobs with attempt 0 should display an informative empty state and avoid requesting issues until the attempt becomes at least 1.

### `GET /import-jobs/{jobId}/issues/export`

Downloads a CSV with **all matching issues**, independent of the currently displayed page. Accepts the same optional `reason_code` and `attempt_number` query parameters. The response is `text/csv` with a `Content-Disposition` attachment filename. Columns are `row_number,sheet_name,reason_code,message,field_name,context,row_data`; `context` and `row_data` are JSON text within CSV cells. If the API client uses bearer headers, fetch this endpoint using that client and download the response as a Blob; a plain link may omit required auth.

## Suggested TypeScript additions

Adapt these to the project's existing types and request helpers rather than duplicating a client.

```ts
type ImportJobIssue = {
  id: number;
  row_number: number;
  sheet_name: string | null;
  reason_code: string;
  message: string;
  field_name: string | null;
  context: Record<string, unknown> | null;
  row_data: Record<string, unknown> | unknown[] | null;
  created_at: string | null;
};

type ImportJobIssueSummary = { reason_code: string; count: number };

type ImportJobIssuesResponse = {
  data: ImportJobIssue[];
  attempt_number: number;
  filters: { reason_code: string | null };
  pagination: {
    current_page: number;
    per_page: number;
    total_pages: number;
    total_records: number;
  };
};

// Extend the existing detail type:
type ImportJobLoggingFields = {
  duplicate_rows: number | null;
  non_duplicate_skipped_rows: number | null;
  attempt_number: number;
  issue_summary: ImportJobIssueSummary[];
};
```

## Detail page behavior

1. Display `Imported`, `Duplicates`, `Skipped — needs review`, and `Failed` with their respective existing/new counters. If a new counter is `null`, display "Not recorded" instead of zero. When a completed job has `non_duplicate_skipped_rows > 0`, a label such as "Completed with issues" is appropriate; its actual API `status` remains `completed`. Keep the job-level `error_message` visible when `status` is `failed`.
2. Add a "Skipped rows needing review" panel beneath the job summary. Use `issue_summary` for reason chips/counts. Show the returned `message` as the primary issue explanation. A human-readable reason label can be derived from `reason_code` by replacing underscores; keep `message` unchanged.
3. Filter by clicking a reason chip or choosing "All reasons". Fetch `GET /issues` with the selected `reason_code`. Reset to page 1 when the filter, job ID, or attempt changes. Drive next/previous controls from `pagination`, not from `skipped_rows`. Show row number and sheet name (when present), reason/message, optional field name, and an expandable rendering of `context` and `row_data`.
4. Offer "Download CSV" for the selected reason or all reasons. Do not include `page` or `per_page` in the export request. Keep the download action disabled or hidden when there are no logged issues.
5. Reuse the page's existing status polling (typically every ~3 seconds for `queued`/`processing`). Refresh the visible issues and summary when status reaches `completed` or `failed`, then stop polling. Avoid stale responses when navigating jobs, changing filters, or unmounting (abort/cleanup according to existing React patterns). If the page loads directly in a terminal state, load the issue list immediately. The API does not guarantee real-time issue entries while a job is still processing.
6. Distinguish empty states: queued (not started), processing (logs may arrive at the end), completed with zero reviewable skips ("No skipped rows need review"), and legacy jobs with unavailable breakdown ("Detailed logs weren't recorded for this import"). Show fetch errors with a retry action without hiding the rest of the job page.

Possible layout:

```text
Import job #123                                     Completed with issues
Processed 500 / 500  |  Imported 438  |  Duplicates 60  |  Needs review 2

Skipped rows needing review                           [Download CSV]
[All (2)]  [Outlet not found (1)]  [Parse error (1)]
Row / Sheet       Reason                 Details
128               Outlet not found       No outlet matches Merchant ID or name  [View row]
231 / Sheet2      Parse error            Invalid date value                   [View row]
                                           Previous  Page 1 of 1  Next
```

## Frontend acceptance checks

- A job with many duplicates and one non-duplicate skip shows duplicates in the counter, **only** the non-duplicate entry in the issues table, and correct CSV contents.
- Reason filtering updates the rows and count; switching reasons resets pagination; export downloads all rows for the active reason, not only the current page.
- A queued or historical job does not send `/issues?attempt_number=0` (which would return 400).
- Failed jobs still show any saved row issues alongside the job-level error; completed clean jobs display a helpful empty state.
- Object and array source rows render safely; Next.js lint/typecheck passes.
