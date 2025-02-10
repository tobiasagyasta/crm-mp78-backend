from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.outlet import Outlet
from app.models.partner import Partner
from datetime import datetime

outlet_bp = Blueprint("outlet_bp", __name__, url_prefix="/outlets")

# Create a new outlet
@outlet_bp.route("", methods=["POST"])
def create_outlet():
    data = request.get_json()

    partner = Partner.query.get(data.get("partner_id"))
    if not partner:
        return jsonify({"error": "Partner not found"}), 404

    outlet = Outlet(
        outlet_name=data["outlet_name"],
        partner_id=partner.id,
        partner_name=partner.name,
        status=data.get("status", "Active"),
        closing_date=datetime.fromisoformat(data["closing_date"]) if data.get("closing_date") else None,
        address=data["address"]
    )

    db.session.add(outlet)
    db.session.commit()
    return jsonify(outlet.to_dict()), 201


# Get all outlets
@outlet_bp.route("", methods=["GET"])
def get_outlets():
    outlets = Outlet.query.all()
    return jsonify([outlet.to_dict() for outlet in outlets])


# Get a single outlet by ID
@outlet_bp.route("/<int:outlet_id>", methods=["GET"])
def get_outlet(outlet_id):
    outlet = Outlet.query.get(outlet_id)
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404
    return jsonify(outlet.to_dict())


# Update an outlet
@outlet_bp.route("/<int:outlet_id>", methods=["PUT"])
def update_outlet(outlet_id):
    outlet = Outlet.query.get(outlet_id)
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404

    data = request.get_json()
    
    # If updating partner, validate new partner
    if "partner_id" in data:
        partner = Partner.query.get(data["partner_id"])
        if not partner:
            return jsonify({"error": "New partner not found"}), 404
        outlet.partner_id = partner.id
        outlet.partner_name = partner.name

    outlet.outlet_name = data.get("outlet_name", outlet.outlet_name)
    outlet.status = data.get("status", outlet.status)
    outlet.closing_date = datetime.fromisoformat(data["closing_date"]) if data.get("closing_date") else outlet.closing_date
    outlet.address = data.get("address", outlet.address)

    db.session.commit()
    return jsonify(outlet.to_dict())


# Delete an outlet
@outlet_bp.route("/<int:outlet_id>", methods=["DELETE"])
def delete_outlet(outlet_id):
    outlet = Outlet.query.get(outlet_id)
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404

    db.session.delete(outlet)
    db.session.commit()
    return jsonify({"message": "Outlet deleted successfully"})
