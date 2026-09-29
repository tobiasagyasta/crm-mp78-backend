from datetime import datetime

from app.extensions import db, s3
from app.models.import_job import ImportJob


def test_import_worker_job(message='ok'):
    return {
        'message': message,
        'processed_at': datetime.utcnow().isoformat() + 'Z',
    }


def process_report_import_job(import_job_id):
    from app import create_app

    app = create_app()

    with app.app_context():
        import_job = ImportJob.query.get(import_job_id)
        if not import_job:
            return {'status': 'missing', 'import_job_id': import_job_id}

        import_job.status = 'processing'
        import_job.started_at = datetime.utcnow()
        db.session.commit()

        try:
            response = s3.client.get_object(
                Bucket=import_job.storage_bucket,
                Key=import_job.storage_key,
            )
            file_bytes = response['Body'].read()

            import_job.status = 'completed'
            import_job.finished_at = datetime.utcnow()
            db.session.commit()

            return {
                'status': 'completed',
                'import_job_id': import_job.id,
                'report_type': import_job.report_type,
                'storage_key': import_job.storage_key,
                'file_size_bytes': len(file_bytes),
            }
        except Exception as exc:
            db.session.rollback()

            import_job = ImportJob.query.get(import_job_id)
            if import_job:
                import_job.status = 'failed'
                import_job.error_message = str(exc)
                import_job.finished_at = datetime.utcnow()
                db.session.commit()

            raise
