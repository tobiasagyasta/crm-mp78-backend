from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.partner import Partner

partner_bp = Blueprint("partner_bp", __name__, url_prefix="/partners")

# Create a new partner
@partner_bp.route("", methods=["POST"])
def create_partner():
    data = request.get_json()

    if not data.get("name") or not data.get("rekening_number"):
        return jsonify({"error": "Name and Rekening Number are required"}), 400

    partner = Partner(
        name=data["name"],
        number_of_outlets=data.get("number_of_outlets", 0),
        rekening_number=data["rekening_number"]
    )

    db.session.add(partner)
    db.session.commit()
    return jsonify(partner.to_dict()), 201


# Get all partners
@partner_bp.route("", methods=["GET"])
def get_partners():
    partners = Partner.query.all()
    return jsonify([partner.to_dict() for partner in partners])


# Get a single partner by ID
@partner_bp.route("/<int:partner_id>", methods=["GET"])
def get_partner(partner_id):
    partner = Partner.query.get(partner_id)
    if not partner:
        return jsonify({"error": "Partner not found"}), 404
    return jsonify(partner.to_dict())


# Update a partner by ID
@partner_bp.route("/<int:partner_id>", methods=["PUT"])
def update_partner(partner_id):
    partner = Partner.query.get(partner_id)
    if not partner:
        return jsonify({"error": "Partner not found"}), 404

    data = request.get_json()
    partner.name = data.get("name", partner.name)
    partner.number_of_outlets = data.get("number_of_outlets", partner.number_of_outlets)
    partner.rekening_number = data.get("rekening_number", partner.rekening_number)

    db.session.commit()
    return jsonify(partner.to_dict())


# Delete a partner by ID
@partner_bp.route("/<int:partner_id>", methods=["DELETE"])
def delete_partner(partner_id):
    partner = Partner.query.get(partner_id)
    if not partner:
        return jsonify({"error": "Partner not found"}), 404

    db.session.delete(partner)
    db.session.commit()
    return jsonify({"message": "Partner deleted successfully"})
