# Next.js Import Jobs Frontend Plan

This document contains low-token OpenCode prompts for implementing the frontend import job flow in a separate Next.js app.

Goal: after a user uploads an import file, redirect them to a job status page that shows current processing progress until the job completes or fails.

Backend endpoints expected:

- `POST /import-jobs/upload/grab`
- `POST /import-jobs/upload/tiktok`
- `POST /import-jobs/upload/mutation`
- `POST /import-jobs/upload/gojek`
- `POST /import-jobs/upload/shopee`
- `GET /import-jobs`
- `GET /import-jobs/{jobId}`

Mutation uploads require a `rekening_number` form field.

## Backend Response Shape

Upload response example:

```json
{
  "msg": "Gojek report upload accepted for background processing",
  "jobs": [
    {
      "import_job_id": 123,
      "rq_job_id": "abc123",
      "status": "queued",
      "storage_key": "docs/gojek/file.csv",
      "original_filename": "gojek.csv"
    }
  ]
}
```

Status response example:

```json
{
  "id": 123,
  "report_type": "gojek",
  "status": "processing",
  "original_filename": "gojek.csv",
  "storage_provider": "railway_bucket",
  "storage_bucket": "bucket-name",
  "storage_key": "docs/gojek/file.csv",
  "extra_data": null,
  "total_rows": 1000,
  "processed_rows": 500,
  "inserted_rows": 480,
  "skipped_rows": 20,
  "failed_rows": 0,
  "error_message": null,
  "created_at": "2026-10-06T10:00:00",
  "started_at": "2026-10-06T10:00:05",
  "finished_at": null,
  "updated_at": "2026-10-06T10:01:00"
}
```

Summary list response example:

```json
{
  "data": [
    {
      "id": 123,
      "report_type": "gojek",
      "status": "processing",
      "original_filename": "gojek.csv",
      "extra_data": null,
      "total_rows": 1000,
      "processed_rows": 500,
      "inserted_rows": 480,
      "skipped_rows": 20,
      "failed_rows": 0,
      "error_message": null,
      "created_at": "2026-10-06T10:00:00",
      "started_at": "2026-10-06T10:00:05",
      "finished_at": null,
      "updated_at": "2026-10-06T10:01:00"
    }
  ],
  "filters": {
    "report_type": null,
    "status": null
  },
  "pagination": {
    "current_page": 1,
    "per_page": 25,
    "total_pages": 1,
    "total_records": 1
  }
}
```

## Implementation Order

Use one prompt at a time. Let OpenCode inspect the codebase before editing.

Recommended order:

1. Discover upload UI and API patterns.
2. Add or update API helper.
3. Create import jobs summary page and job detail page.
4. Add polling.
5. Add progress bar.
6. Redirect after upload.
7. Add mutation `rekening_number` field.
8. Verify with lint/typecheck/build.

## Prompt 1: Find Existing Upload UI

```text
Find the existing report upload pages/components and identify where report uploads are handled.

Look specifically for Grab, TikTok, Gojek, Shopee, mutation, or generic upload UI/API code.

Do not edit files yet.

Return:
- relevant file paths
- which component handles upload submit
- existing API helper/client patterns
- whether the project uses app router or pages router
- the likely minimal files to edit
```

## Prompt 2: Add API Helper

```text
Add or update the frontend API helper for import job uploads.

Requirements:
- Support report types: grab, tiktok, mutation, gojek, shopee
- Send multipart FormData to /import-jobs/upload/{type}
- Include rekening_number only for mutation
- Return the backend JSON response
- Follow existing API client/base URL/auth patterns in this repo
- Keep the change minimal

Types to use if TypeScript is available:

type ImportJobUploadType = "grab" | "tiktok" | "mutation" | "gojek" | "shopee";

type ImportJobUploadResponse = {
  msg: string;
  jobs: Array<{
    import_job_id: number;
    rq_job_id: string;
    status: string;
    storage_key: string;
    original_filename: string | null;
  }>;
};
```

## Prompt 3: Create Summary And Detail Pages

```text
Create two Next.js pages for import jobs:

1. Summary page at /import-jobs
2. Detail page at /import-jobs/[jobId]

Use app router or pages router based on this repo.

Summary page requirements:
- Fetch GET /import-jobs?page=1&per_page=25
- Show a table/list of recent import jobs
- Each row should show job id, report_type, status, original_filename, processed_rows, total_rows, inserted_rows, skipped_rows, failed_rows, created_at, updated_at
- Each row should link to /import-jobs/{id}
- Add simple filters for status and report_type if this is easy using existing UI patterns
- Keep pagination minimal: next/previous buttons are enough

Detail page requirements:
- Fetch GET /import-jobs/{jobId}
- Show a more detailed breakdown for a single job
- Show:
- job id
- report_type
- status
- original_filename
- storage_key if useful
- total_rows
- processed_rows
- inserted_rows
- skipped_rows
- failed_rows
- error_message if failed
- created_at
- started_at
- finished_at

Use existing UI components/styles if available.
Keep the first version simple and readable.
```

Summary list TypeScript shape:

```ts
type ImportJobsListResponse = {
  data: ImportJob[];
  filters: {
    report_type: string | null;
    status: string | null;
  };
  pagination: {
    current_page: number;
    per_page: number;
    total_pages: number;
    total_records: number;
  };
};
```

Suggested TypeScript shape:

```ts
type ImportJob = {
  id: number;
  report_type: string;
  status: "queued" | "processing" | "completed" | "failed" | string;
  original_filename: string | null;
  storage_provider?: string | null;
  storage_bucket?: string | null;
  storage_key?: string | null;
  extra_data?: Record<string, unknown> | null;
  total_rows: number;
  processed_rows: number;
  inserted_rows: number;
  skipped_rows: number;
  failed_rows: number;
  error_message: string | null;
  created_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
};
```

## Prompt 4: Add Polling

```text
Update import job pages with polling.

Summary page:
- Poll GET /import-jobs every 5 seconds while at least one visible job is queued or processing.
- Stop polling if all visible jobs are completed or failed.

Detail page:
- Poll GET /import-jobs/{jobId} every 3 seconds while status is queued or processing.
- Stop polling when status is completed or failed.

Avoid unnecessary requests after terminal states.
Clean up intervals on unmount.
Keep the implementation minimal and consistent with this repo's React patterns.
```

## Prompt 5: Add Progress Bar

```text
Add a progress bar to the import job status page.

Progress formula:
- if total_rows > 0: Math.round(processed_rows / total_rows * 100)
- if status is completed: 100
- otherwise: 0

Show the percentage text and row counts near the progress bar.

Use existing UI primitives if available. If no progress component exists, use simple div-based styling consistent with the app.
```

## Prompt 6: Redirect After Upload

```text
Update the upload flow so successful import job upload redirects to /import-jobs/{import_job_id}.

Use the first returned job from response.jobs.

Requirements:
- Keep existing upload UI as-is where possible
- Keep existing loading/error behavior where possible
- If multiple files are uploaded, redirect to the first job for now
- Do not redesign the page
- Use the router pattern already used in this repo
```

## Prompt 7: Add Mutation Rekening Number Field

```text
If the upload UI supports selecting mutation upload, add a rekening_number input.

Requirements:
- Required only when report type is mutation
- Sent as FormData field named rekening_number
- Not required for grab, tiktok, gojek, or shopee
- Show a clear validation message if missing
- Keep styling consistent with existing form fields
```

## Prompt 8: Verify

```text
Run the available frontend verification command.

Prefer, in order:
- npm run lint
- npm run typecheck
- npm run build

Use whatever scripts exist in package.json.

If verification fails, fix only issues caused by the import job UI changes.
Return a short summary of commands run and results.
```

## Suggested UI Layout

The summary page should have these sections:

- Header: `Import Jobs`, upload action if useful.
- Filters: status and report type.
- Table/list: recent jobs with file, type, status, row counters, created/updated time.
- Progress: compact percentage per row.
- Pagination: previous and next.

The detail page should have these sections:

- Header: `Import Job #123`, status badge, report type.
- File summary: filename and storage key if useful.
- Progress: progress bar, percentage, processed/total rows.
- Counters: inserted, skipped, failed.
- Timeline: created, started, finished.
- Error panel: visible only when status is `failed`.
- Actions: back to uploads, retry upload, view reports.

## Polling Rules

Terminal statuses:

- `completed`
- `failed`

Active statuses:

- `queued`
- `processing`

Polling interval:

- 3 seconds is enough.
- Do not poll faster unless there is a strong reason.

## Minimal Fetch Example

```ts
async function listImportJobs(params?: {
  page?: number;
  perPage?: number;
  status?: string;
  reportType?: string;
}) {
  const searchParams = new URLSearchParams();
  searchParams.set("page", String(params?.page ?? 1));
  searchParams.set("per_page", String(params?.perPage ?? 25));

  if (params?.status) searchParams.set("status", params.status);
  if (params?.reportType) searchParams.set("report_type", params.reportType);

  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/import-jobs?${searchParams}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    throw new Error("Failed to fetch import jobs");
  }

  return res.json();
}

async function getImportJob(jobId: string) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/import-jobs/${jobId}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    throw new Error("Failed to fetch import job");
  }

  return res.json();
}
```

## Minimal Upload Example

```ts
async function uploadImportJob(params: {
  type: "grab" | "tiktok" | "mutation" | "gojek" | "shopee";
  files: File[];
  rekeningNumber?: string;
}) {
  const formData = new FormData();

  for (const file of params.files) {
    formData.append("file", file);
  }

  if (params.type === "mutation") {
    formData.append("rekening_number", params.rekeningNumber ?? "");
  }

  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/import-jobs/upload/${params.type}`, {
    method: "POST",
    body: formData,
  });

  if (!res.ok) {
    throw new Error("Failed to upload import job");
  }

  return res.json();
}
```

## Backend Limitation To Remember

The current backend status endpoint returns counters and `error_message`, but not detailed per-row logs.

If the frontend needs detailed skipped-row reasons or per-step logs, implement a later backend enhancement:

```text
Add result_data JSONB to import_jobs, store importer result debug data, and return it from GET /import-jobs/{id}.
```

Do not implement that frontend UI until the backend returns the data.
