from datetime import datetime


def test_import_worker_job(message='ok'):
    return {
        'message': message,
        'processed_at': datetime.utcnow().isoformat() + 'Z',
    }
