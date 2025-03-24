from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.income_category import IncomeCategory

income_category_bp = Blueprint("income_category_bp", __name__, url_prefix="/income_categories")

# Create a new income category
@income_category_bp.route("", methods=["POST"])
def create_income_category():
    data = request.get_json()

    if not data.get("name"):
        return jsonify({"error": "Income category name is required"}), 400
    
    if IncomeCategory.query.filter_by(name=data["name"]).first():
        return jsonify({"error": "Income category already exists"}), 400

    category = IncomeCategory(name=data["name"])
    db.session.add(category)
    db.session.commit()

    return jsonify(category.to_dict()), 201

# Get all income categories
@income_category_bp.route("", methods=["GET"])
def get_income_categories():
    categories = IncomeCategory.query.all()
    return jsonify([category.to_dict() for category in categories])

# Get a single income category by ID
@income_category_bp.route("/<int:category_id>", methods=["GET"])
def get_income_category(category_id):
    category = IncomeCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Income category not found"}), 404
    return jsonify(category.to_dict())

# Update an income category
@income_category_bp.route("/<int:category_id>", methods=["PUT"])
def update_income_category(category_id):
    category = IncomeCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Income category not found"}), 404

    data = request.get_json()
    if "name" in data and data["name"]:
        category.name = data["name"]
    
    db.session.commit()
    return jsonify(category.to_dict())

# Delete an income category
@income_category_bp.route("/<int:category_id>", methods=["DELETE"])
def delete_income_category(category_id):
    category = IncomeCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Income category not found"}), 404

    db.session.delete(category)
    db.session.commit()
    return jsonify({"message": "Income category deleted successfully"})
