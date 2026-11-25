# Merchant import jobs: frontend handoff

The backend now supports the remaining merchant report uploads as background
import jobs. In the separate Next.js frontend, add these options to the existing
import-jobs upload form and report-type filters:

| Merchant report | POST endpoint | `report_type` |
| --- | --- | --- |
| Shopee Adjustment | `/import-jobs/upload/shopee-adjustment` | `shopee_adjustment` |
| ShopeePay | `/import-jobs/upload/shopeepay` | `shopeepay` |
| Qpon | `/import-jobs/upload/qpon` | `qpon` |
| Webshop | `/import-jobs/upload/webshop` | `webshop` |

Use the same multipart `file` field, 202 response, job polling, counters, and
issue views as the existing merchant import jobs. Multiple files create separate
jobs. The Webshop upload also imports Maxim rows from the shared export; no
separate Maxim upload is needed.

The existing synchronous `/reports/upload/...` endpoints remain available for
older clients. Update the frontend to use the import-job endpoints to move these
uploads into the jobs section.
