from flask import Flask, jsonify
from app.config.config import Config
from app.extensions import db, jwt
from app.controllers.auth_controller import auth_bp
from app.controllers.protected_controller import protected_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize extensions AFTER app creation
    db.init_app(app)
    jwt.init_app(app)

    # Register Blueprints (Routes)
    app.register_blueprint(auth_bp)
    app.register_blueprint(protected_bp)

    @app.route('/')
    def welcome():
        return jsonify({'message': 'Welcome to the Flask Auth API'})

    return app

if __name__ == '__main__':
    app = create_app()
    with app.app_context():
        db.create_all()
    app.run(debug=True)
