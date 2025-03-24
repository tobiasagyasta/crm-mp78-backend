from flask import Flask, jsonify
from app.config.config import Config
from app.extensions import db, jwt
from app.controllers.auth_controller import auth_bp
from app.controllers.protected_controller import protected_bp
from app.controllers.partner_controller import partner_bp
from app.controllers.outlet_controller import outlet_bp
from app.controllers.product_controller import product_bp
from app.controllers.expense_category_controller import expense_category_bp
from app.controllers.income_category_controller import income_category_bp
from app.controllers.reports_controller import reports_bp
from app.controllers.manual_entry_controller import manual_entries_bp
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize extensions AFTER app creation
    db.init_app(app)
    jwt.init_app(app)

     # Enable CORS for all domains and ports
    CORS(app)

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


    @app.route('/')
    def welcome():
        return jsonify({'message': 'Welcome to the MP78 API'})
    

    return app

if __name__ == '__main__':
    app = create_app()
    with app.app_context():
        db.create_all()
    app.run(debug=True)
