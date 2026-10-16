import json

from app.extensions import db
from app.models.import_job_issue import ImportJobIssue


class IssueCollector:
    """Collect row diagnostics until the worker can safely commit them.

    Keeping them outside the report transaction lets the worker save them after
    a rollback too. The first 50 are returned for legacy direct-import callers.
    """

    def __init__(self):
        self.pending = []
        self.preview = []

    def record(self, row_number, reason_code, message, row, *, field_name=None,
               context=None, sheet_name=None):
        # CSV DictReader may have None keys; Excel may contain datetime objects.
        row_data = json.loads(json.dumps(row, default=str))
        if isinstance(row_data, dict):
            row_data = {str(key): value for key, value in row_data.items()}
        entry = {
            'row_number': row_number,
            'sheet_name': sheet_name,
            'reason_code': reason_code,
            'message': message,
            'field_name': field_name,
            'context': json.loads(json.dumps(context, default=str)) if context else None,
            'row_data': row_data,
        }
        self.pending.append(entry)
        if len(self.preview) < 50:
            self.preview.append({'row_number': row_number, 'reason': message, 'row': row_data})

    def save(self, job_id, attempt_number):
        for start in range(0, len(self.pending), 500):
            db.session.bulk_insert_mappings(ImportJobIssue, [
                {'import_job_id': job_id, 'attempt_number': attempt_number, **entry}
                for entry in self.pending[start:start + 500]
            ])
