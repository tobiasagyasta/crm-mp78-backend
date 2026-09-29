from uuid import uuid4

from flask import Blueprint, jsonify, request
from werkzeug.utils import secure_filename

from app.extensions import db, s3
from app.extensions.queue import get_import_queue
from app.models.import_job import ImportJob
from app.workers.report_import_jobs import process_report_import_job


import_jobs_bp = Blueprint('import_jobs', __name__, url_prefix='/import-jobs')


@import_jobs_bp.route('/upload/grab', methods=['POST'])
def upload_grab_import_job():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]

    files = [file for file in files if file and file.filename]
    if not files:
        return jsonify({'msg': 'No files uploaded'}), 400

    import_jobs = []

    try:
        for file in files:
            original_filename = secure_filename(file.filename or 'grab-report.csv')
            storage_key = f'docs/grab/{uuid4().hex}-{original_filename}'

            s3.client.upload_fileobj(
                file,
                s3.bucket,
                storage_key,
            )

            import_job = ImportJob(
                report_type='grab',
                status='queued',
                original_filename=original_filename,
                storage_provider='railway_bucket',
                storage_bucket=s3.bucket,
                storage_key=storage_key,
            )
            db.session.add(import_job)
            db.session.flush()

            import_jobs.append(import_job)

        db.session.commit()

        queue = get_import_queue()
        response_jobs = []
        for import_job in import_jobs:
            rq_job = queue.enqueue(
                process_report_import_job,
                import_job.id,
                job_timeout=1800,
                result_ttl=300,
            )
            response_jobs.append({
                'import_job_id': import_job.id,
                'rq_job_id': rq_job.id,
                'status': import_job.status,
                'storage_key': import_job.storage_key,
                'original_filename': import_job.original_filename,
            })

        return jsonify({
            'msg': 'Grab report upload accepted for background processing',
            'jobs': response_jobs,
        }), 202

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@import_jobs_bp.route('/<int:job_id>', methods=['GET'])
def get_import_job(job_id):
    import_job = ImportJob.query.get_or_404(job_id)

    return jsonify({
        'id': import_job.id,
        'report_type': import_job.report_type,
        'status': import_job.status,
        'original_filename': import_job.original_filename,
        'storage_provider': import_job.storage_provider,
        'storage_bucket': import_job.storage_bucket,
        'storage_key': import_job.storage_key,
        'total_rows': import_job.total_rows,
        'processed_rows': import_job.processed_rows,
        'inserted_rows': import_job.inserted_rows,
        'skipped_rows': import_job.skipped_rows,
        'failed_rows': import_job.failed_rows,
        'error_message': import_job.error_message,
        'created_at': import_job.created_at.isoformat() if import_job.created_at else None,
        'started_at': import_job.started_at.isoformat() if import_job.started_at else None,
        'finished_at': import_job.finished_at.isoformat() if import_job.finished_at else None,
        'updated_at': import_job.updated_at.isoformat() if import_job.updated_at else None,
    })
