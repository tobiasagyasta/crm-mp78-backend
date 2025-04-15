from flask import Flask, jsonify
from app.config.config import Config
from app.config.env import init_env
from app.extensions import db, jwt, s3
from app.controllers.auth_controller import auth_bp
from app.controllers.protected_controller import protected_bp
from app.controllers.partner_controller import partner_bp
from app.controllers.outlet_controller import outlet_bp
from app.controllers.product_controller import product_bp
from app.controllers.expense_category_controller import expense_category_bp
from app.controllers.income_category_controller import income_category_bp
from app.controllers.reports_controller import reports_bp
from app.controllers.manual_entry_controller import manual_entries_bp
from app.controllers.export_controller import export_bp
from app.controllers.reports_controller_s3 import reports_s3_bp
from app.controllers.mutations_controller import mutations_bp
from flask_cors import CORS

def create_app():
    # Initialize environment variables
    init_env()
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize extensions AFTER app creation
    db.init_app(app)
    jwt.init_app(app)
    s3.init_app(app)

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
    app.register_blueprint(expense_category_bp)
    app.register_blueprint(income_category_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(manual_entries_bp)
    app.register_blueprint(export_bp)
    app.register_blueprint(reports_s3_bp)
    app.register_blueprint(mutations_bp)

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
    
    @app.route('/test-s3-upload')
    def test_s3_upload():
        try:
            # Create a simple CSV content
            csv_content = "id,name,value\n1,test,100\n2,sample,200"
            
            # Upload the CSV content
            s3.client.put_object(
                Bucket=s3.bucket,
                Key='foo.csv',
                Body=csv_content,
                ContentType='text/csv'
            )
            
            return jsonify({
                'status': 'success',
                'message': 'Successfully uploaded foo.csv to S3',
                'file_name': 'foo.csv',
                'bucket': s3.bucket
            }), 200
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Failed to upload file: {str(e)}'
            }), 500

    return app
