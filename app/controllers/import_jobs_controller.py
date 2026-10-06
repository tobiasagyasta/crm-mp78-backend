import os
from uuid import uuid4

from flask import Blueprint, current_app, jsonify, request
from werkzeug.utils import secure_filename

from app.extensions import db, s3
from app.extensions.queue import get_import_queue
from app.models.import_job import ImportJob
from app.workers.report_import_jobs import process_report_import_job


import_jobs_bp = Blueprint('import_jobs', __name__, url_prefix='/import-jobs')


def _serialize_import_job(import_job, include_storage=True):
    data = {
        'id': import_job.id,
        'report_type': import_job.report_type,
        'status': import_job.status,
        'original_filename': import_job.original_filename,
        'extra_data': import_job.extra_data,
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
    }

    if include_storage:
        data.update({
            'storage_provider': import_job.storage_provider,
            'storage_bucket': import_job.storage_bucket,
            'storage_key': import_job.storage_key,
        })

    return data


def _is_import_job_simulator_enabled():
    return (
        current_app.debug
        or current_app.testing
        or os.getenv('FLASK_ENV') == 'development'
        or os.getenv('APP_ENV') in ('dev', 'development', 'local')
        or os.getenv('ENABLE_IMPORT_JOB_SIMULATOR') == '1'
    )


def _upload_report_import_job(report_type, extra_data=None):
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]

    files = [file for file in files if file and file.filename]
    if not files:
        return jsonify({'msg': 'No files uploaded'}), 400

    import_jobs = []

    try:
        for file in files:
            original_filename = secure_filename(file.filename or f'{report_type}-report.csv')
            storage_key = f'docs/{report_type}/{uuid4().hex}-{original_filename}'

            s3.client.upload_fileobj(
                file,
                s3.bucket,
                storage_key,
            )

            import_job = ImportJob(
                report_type=report_type,
                status='queued',
                original_filename=original_filename,
                storage_provider='railway_bucket',
                storage_bucket=s3.bucket,
                storage_key=storage_key,
                extra_data=extra_data,
            )
            db.session.add(import_job)
            db.session.flush()

            import_jobs.append(import_job)

        db.session.commit()

        queue = get_import_queue()
        response_jobs = []
        for import_job in import_jobs:
            job_timeout = 7200 if report_type in ('tiktok', 'mutation') else 1800
            rq_job = queue.enqueue(
                process_report_import_job,
                import_job.id,
                job_timeout=job_timeout,
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
            'msg': f'{report_type.title()} report upload accepted for background processing',
            'jobs': response_jobs,
        }), 202

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@import_jobs_bp.route('/upload/grab', methods=['POST'])
def upload_grab_import_job():
    return _upload_report_import_job('grab')


@import_jobs_bp.route('/upload/tiktok', methods=['POST'])
def upload_tiktok_import_job():
    return _upload_report_import_job('tiktok')


@import_jobs_bp.route('/upload/mutation', methods=['POST'])
def upload_mutation_import_job():
    rekening_number = request.form.get('rekening_number')
    if not rekening_number:
        return jsonify({'msg': 'Rekening number is required'}), 400

    return _upload_report_import_job('mutation', {'rekening_number': rekening_number})


@import_jobs_bp.route('/upload/gojek', methods=['POST'])
def upload_gojek_import_job():
    return _upload_report_import_job('gojek')


@import_jobs_bp.route('/upload/shopee', methods=['POST'])
def upload_shopee_import_job():
    return _upload_report_import_job('shopee')


@import_jobs_bp.route('', methods=['GET'])
def list_import_jobs():
    page = max(request.args.get('page', 1, type=int), 1)
    per_page = min(max(request.args.get('per_page', 25, type=int), 1), 100)
    report_type = request.args.get('report_type')
    status = request.args.get('status')

    query = ImportJob.query
    if report_type:
        query = query.filter(ImportJob.report_type == report_type)
    if status:
        query = query.filter(ImportJob.status == status)

    total_records = query.count()
    total_pages = (total_records + per_page - 1) // per_page
    import_jobs = (
        query
        .order_by(ImportJob.created_at.desc(), ImportJob.id.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return jsonify({
        'data': [
            _serialize_import_job(import_job, include_storage=False)
            for import_job in import_jobs
        ],
        'filters': {
            'report_type': report_type,
            'status': status,
        },
        'pagination': {
            'current_page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_records': total_records,
        },
    })


@import_jobs_bp.route('/<int:job_id>', methods=['GET'])
def get_import_job(job_id):
    import_job = ImportJob.query.get_or_404(job_id)

    return jsonify(_serialize_import_job(import_job))


@import_jobs_bp.route('/<int:job_id>/simulate-worker', methods=['POST'])
def simulate_import_job_worker(job_id):
    if not _is_import_job_simulator_enabled():
        return jsonify({
            'error': 'Import job worker simulator is disabled',
            'hint': 'Enable Flask debug/testing/development mode or set ENABLE_IMPORT_JOB_SIMULATOR=1',
        }), 403

    import_job = ImportJob.query.get_or_404(job_id)
    if import_job.status == 'processing':
        return jsonify({'error': 'Import job is already processing'}), 409
    if import_job.status == 'completed':
        return jsonify({'error': 'Import job is already completed'}), 409

    try:
        result = process_report_import_job(job_id)
        refreshed_job = ImportJob.query.get(job_id)

        return jsonify({
            'msg': 'Import job worker simulation completed',
            'job': _serialize_import_job(refreshed_job) if refreshed_job else None,
            'result': result,
        }), 200
    except Exception as exc:
        refreshed_job = ImportJob.query.get(job_id)

        return jsonify({
            'error': str(exc),
            'job': _serialize_import_job(refreshed_job) if refreshed_job else None,
        }), 500
