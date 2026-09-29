from datetime import datetime

from app.extensions import db, s3
from app.models.import_job import ImportJob
from app.services.grab_report_importer import import_grab_report_bytes


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

            if import_job.report_type == 'grab':
                result = import_grab_report_bytes(file_bytes, import_job_id=import_job.id)
            else:
                raise ValueError(f'Unsupported report type: {import_job.report_type}')

            import_job.status = 'completed'
            import_job.total_rows = result.get('total_rows', 0)
            import_job.processed_rows = result.get('processed_rows', 0)
            import_job.inserted_rows = result.get('inserted_rows', 0)
            import_job.skipped_rows = result.get('skipped_rows', 0)
            import_job.failed_rows = result.get('failed_rows', 0)
            import_job.finished_at = datetime.utcnow()
            db.session.commit()

            return {
                'status': 'completed',
                'import_job_id': import_job.id,
                'report_type': import_job.report_type,
                'storage_key': import_job.storage_key,
                'file_size_bytes': len(file_bytes),
                'result': result,
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
