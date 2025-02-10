from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.expense_category import ExpenseCategory

expense_category_bp = Blueprint("expense_category_bp", __name__, url_prefix="/expense_categories")

# Create a new expense category
@expense_category_bp.route("", methods=["POST"])
def create_expense_category():
    data = request.get_json()

    if not data.get("name"):
        return jsonify({"error": "Expense category name is required"}), 400
    
    if ExpenseCategory.query.filter_by(name=data["name"]).first():
        return jsonify({"error": "Expense category already exists"}), 400

    category = ExpenseCategory(name=data["name"])
    db.session.add(category)
    db.session.commit()

    return jsonify(category.to_dict()), 201

# Get all expense categories
@expense_category_bp.route("", methods=["GET"])
def get_expense_categories():
    categories = ExpenseCategory.query.all()
    return jsonify([category.to_dict() for category in categories])

# Get a single expense category by ID
@expense_category_bp.route("/<int:category_id>", methods=["GET"])
def get_expense_category(category_id):
    category = ExpenseCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Expense category not found"}), 404
    return jsonify(category.to_dict())

# Update an expense category
@expense_category_bp.route("/<int:category_id>", methods=["PUT"])
def update_expense_category(category_id):
    category = ExpenseCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Expense category not found"}), 404

    data = request.get_json()
    if "name" in data and data["name"]:
        category.name = data["name"]
    
    db.session.commit()
    return jsonify(category.to_dict())

# Delete an expense category
@expense_category_bp.route("/<int:category_id>", methods=["DELETE"])
def delete_expense_category(category_id):
    category = ExpenseCategory.query.get(category_id)
    if not category:
        return jsonify({"error": "Expense category not found"}), 404

    db.session.delete(category)
    db.session.commit()
    return jsonify({"message": "Expense category deleted successfully"})
