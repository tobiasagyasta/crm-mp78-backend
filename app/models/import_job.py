from datetime import datetime

from app.extensions import db


class ImportJob(db.Model):
    __tablename__ = 'import_jobs'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    report_type = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(30), nullable=False, default='queued')

    original_filename = db.Column(db.String(255), nullable=True)
    storage_provider = db.Column(db.String(50), nullable=False, default='railway_bucket')
    storage_bucket = db.Column(db.String(255), nullable=False)
    storage_key = db.Column(db.String(500), nullable=False)

    total_rows = db.Column(db.Integer, nullable=False, default=0)
    processed_rows = db.Column(db.Integer, nullable=False, default=0)
    inserted_rows = db.Column(db.Integer, nullable=False, default=0)
    skipped_rows = db.Column(db.Integer, nullable=False, default=0)
    failed_rows = db.Column(db.Integer, nullable=False, default=0)

    error_message = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    def __repr__(self):
        return f"<ImportJob {self.id} {self.report_type} {self.status}>"
