from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.product import Product

product_bp = Blueprint("product_bp", __name__, url_prefix="/products")

# Create a new product
@product_bp.route("", methods=["POST"])
def create_product():
    data = request.get_json()
    
    if not data.get("name"):
        return jsonify({"error": "Product name is required"}), 400
    
    if Product.query.filter_by(name=data["name"]).first():
        return jsonify({"error": "Product name already exists"}), 400

    product = Product(name=data["name"])
    db.session.add(product)
    db.session.commit()

    return jsonify(product.to_dict()), 201

# Get all products
@product_bp.route("", methods=["GET"])
def get_products():
    products = Product.query.order_by(Outlet.name.asc()).all()
    return jsonify([product.to_dict() for product in products])

# Get a single product by ID
@product_bp.route("/<int:product_id>", methods=["GET"])
def get_product(product_id):
    product = Product.query.get(product_id)
    if not product:
        return jsonify({"error": "Product not found"}), 404
    return jsonify(product.to_dict())

# Update a product
@product_bp.route("/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    product = Product.query.get(product_id)
    if not product:
        return jsonify({"error": "Product not found"}), 404

    data = request.get_json()
    if "name" in data and data["name"]:
        product.name = data["name"]
    
    db.session.commit()
    return jsonify(product.to_dict())

# Delete a product
@product_bp.route("/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    product = Product.query.get(product_id)
    if not product:
        return jsonify({"error": "Product not found"}), 404

    db.session.delete(product)
    db.session.commit()
    return jsonify({"message": "Product deleted successfully"})
