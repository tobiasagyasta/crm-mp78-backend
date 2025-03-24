from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.user import User

protected_bp = Blueprint('protected_bp', __name__)

@protected_bp.route('/protected', methods=['GET'])
@jwt_required()
def protected():
    try:
        current_user_id = int(get_jwt_identity())  # Explicitly convert to int
    except ValueError:
        return jsonify({"msg": "Invalid user ID"}), 400  # Handle invalid cases
    
    user = User.query.get(current_user_id)
    
    if not user:
        return jsonify({"msg": "User not found"}), 404

    return jsonify(user.to_dict())
