from datetime import datetime
from flask import Flask, jsonify, request
from app.config.env import init_env
from app.extensions import db, jwt, s3
from app.controllers.auth_controller import auth_bp
from app.controllers.admin_tools_controller import admin_tools_bp
from app.controllers.protected_controller import protected_bp
from app.controllers.partner_controller import partner_bp
from app.controllers.outlet_controller import outlet_bp
from app.controllers.product_controller import product_bp
from app.controllers.rekening_controller import rekening_bp
from app.controllers.expense_category_controller import expense_category_bp
from app.controllers.income_category_controller import income_category_bp
from app.controllers.reports_controller import reports_bp
from app.controllers.import_jobs_controller import import_jobs_bp
from app.controllers.manual_entry_controller import manual_entries_bp
from app.controllers.export_controller import export_bp
from app.controllers.reports_controller_s3 import reports_s3_bp
from app.controllers.mutations_controller import mutations_bp
from app.controllers.summary_controller import summary_bp
from app.controllers.test_controller import test_bp
from app.controllers.bi_controller import bi_bp
from app.controllers import qpon_controller
from app.controllers import webshop_controller
from app.models.transaction_match import TransactionMatch
from flask_cors import CORS

def create_app():
    # Initialize environment variables
    init_env()
    from app.config.config import Config
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize extensions AFTER app creation
    db.init_app(app)
    jwt.init_app(app)
    s3.init_app(app)

    origins = [
        "https://crm-mp78-frontend.vercel.app",  # Your production Vercel URL
        "http://localhost:3000"                  # Your local development URL
    ]


    # Configure CORS globally with all necessary settings
    CORS(app, resources={
        r"/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"],
            "expose_headers": ["Content-Disposition", "Content-Type"],
            "supports_credentials": True
        }
    })

    # Remove the duplicate CORS configuration
    # CORS(export_bp, supports_credentials=True, expose_headers=["Content-Disposition"])

    # Register Blueprints (Routes)
    app.register_blueprint(auth_bp)
    app.register_blueprint(protected_bp)
    app.register_blueprint(partner_bp)
    app.register_blueprint(outlet_bp)
    app.register_blueprint(product_bp)
    app.register_blueprint(rekening_bp)
    app.register_blueprint(expense_category_bp)
    app.register_blueprint(income_category_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(import_jobs_bp)
    app.register_blueprint(manual_entries_bp)
    app.register_blueprint(export_bp)
    app.register_blueprint(reports_s3_bp)
    app.register_blueprint(mutations_bp)
    app.register_blueprint(summary_bp)
    app.register_blueprint(test_bp)
    app.register_blueprint(bi_bp)
    app.register_blueprint(admin_tools_bp)
    
    @app.route('/')
    def welcome():
        return jsonify({'message': 'Welcome to the MP78 API'})
    
    @app.route('/test-s3')
    def test_s3_connection():
        try:
            # List objects in bucket
            response = s3.client.list_objects_v2(Bucket=s3.bucket, MaxKeys=1)
            
            return jsonify({
                'status': 'success',
                'message': 'Successfully connected to S3',
                'bucket': s3.bucket,
                'sample_contents': response.get('Contents', [])
            }), 200
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Failed to connect to S3: {str(e)}'
            }), 500
    
    @app.route('/test-s3-grab-write')
    def test_s3_grab_write():
        try:
            key = 'docs/grab/railway-bucket-connection-test.txt'
            content = (
                'Railway bucket connection test\n'
                f'created_at={datetime.utcnow().isoformat()}Z\n'
            )

            s3.client.put_object(
                Bucket=s3.bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType='text/plain'
            )

            response = s3.client.get_object(Bucket=s3.bucket, Key=key)
            downloaded_content = response['Body'].read().decode('utf-8')

            s3.client.delete_object(Bucket=s3.bucket, Key=key)

            return jsonify({
                'status': 'success',
                'message': 'Successfully wrote, read, and deleted a test object in docs/grab',
                'bucket': s3.bucket,
                'key': key,
                'downloaded_content': downloaded_content
            }), 200

        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Failed to write/read/delete test object: {str(e)}'
            }), 500

    @app.route('/test-redis')
    def test_redis_connection():
        try:
            from app.extensions.queue import get_import_queue
            from app.workers.report_import_jobs import test_import_worker_job

            queue = get_import_queue()
            ping_result = queue.connection.ping()
            enqueue_value = request.args.get('enqueue')

            response = {
                'status': 'success',
                'message': 'Successfully connected to Redis',
                'queue': queue.name,
                'ping': ping_result,
                'request': {
                    'args': request.args.to_dict(),
                    'enqueue_value': enqueue_value,
                    'enqueue_requested': enqueue_value == '1',
                    'path': request.path,
                    'full_path': request.full_path,
                    'query_string': request.query_string.decode('utf-8'),
                },
            }

            if enqueue_value == '1':
                job = queue.enqueue(
                    test_import_worker_job,
                    'redis-worker-test',
                    job_timeout=60,
                    result_ttl=300,
                )
                response['job'] = {
                    'id': job.id,
                    'status': job.get_status(),
                }

            return jsonify(response), 200

        except ImportError as e:
            return jsonify({
                'status': 'error',
                'message': f'Redis/RQ dependency import failed: {str(e)}'
            }), 500
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Failed to connect to Redis: {str(e)}'
            }), 500

    return app
