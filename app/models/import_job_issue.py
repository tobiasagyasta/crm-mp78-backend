from datetime import datetime

from app.extensions import db


class ImportJobIssue(db.Model):
    __tablename__ = 'import_job_issues'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    import_job_id = db.Column(db.Integer, db.ForeignKey('import_jobs.id'), nullable=False, index=True)
    attempt_number = db.Column(db.Integer, nullable=False)
    row_number = db.Column(db.Integer, nullable=False)
    sheet_name = db.Column(db.String(255))
    reason_code = db.Column(db.String(80), nullable=False)
    message = db.Column(db.Text, nullable=False)
    field_name = db.Column(db.String(255))
    context = db.Column(db.JSON)
    row_data = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.Index('ix_import_job_issues_job_attempt_reason', 'import_job_id', 'attempt_number', 'reason_code'),
    )
