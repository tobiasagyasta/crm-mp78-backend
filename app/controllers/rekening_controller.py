from flask import Blueprint, jsonify, request
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.rekening import Rekening

rekening_bp = Blueprint("rekening_bp", __name__, url_prefix="/rekenings")


def _to_dict(rekening: Rekening) -> dict:
    return {
        "id": rekening.id,
        "name": rekening.name,
        "rekening_type": rekening.rekening_type,
        "rekening_number": rekening.rekening_number,
    }


@rekening_bp.route("", methods=["POST"])
def create_rekening():
    data = request.get_json() or {}

    name = (data.get("name") or "").strip()
    rekening_number = (data.get("rekening_number") or "").strip()
    rekening_type = data.get("rekening_type")

    if isinstance(rekening_type, str):
        rekening_type = rekening_type.strip() or None

    if not name or not rekening_number:
        return jsonify({"error": "Name and rekening_number are required"}), 400

    if Rekening.query.filter_by(rekening_number=rekening_number).first():
        return jsonify({"error": "Rekening number already exists"}), 400

    rekening = Rekening(
        name=name,
        rekening_type=rekening_type,
        rekening_number=rekening_number,
    )
    db.session.add(rekening)

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify({"error": "Rekening number already exists"}), 400

    return jsonify(_to_dict(rekening)), 201


@rekening_bp.route("", methods=["GET"])
def get_rekenings():
    rekenings = Rekening.query.order_by(Rekening.id.asc()).all()
    return jsonify([_to_dict(rekening) for rekening in rekenings]), 200


@rekening_bp.route("/<int:rekening_id>", methods=["GET"])
def get_rekening(rekening_id):
    rekening = Rekening.query.get(rekening_id)
    if not rekening:
        return jsonify({"error": "Rekening not found"}), 404

    return jsonify(_to_dict(rekening)), 200


@rekening_bp.route("/<int:rekening_id>", methods=["PUT"])
def update_rekening(rekening_id):
    rekening = Rekening.query.get(rekening_id)
    if not rekening:
        return jsonify({"error": "Rekening not found"}), 404

    data = request.get_json() or {}

    if "name" in data:
        name = (data.get("name") or "").strip()
        if not name:
            return jsonify({"error": "Name cannot be empty"}), 400
        rekening.name = name

    if "rekening_type" in data:
        rekening_type = data.get("rekening_type")
        if isinstance(rekening_type, str):
            rekening_type = rekening_type.strip() or None
        rekening.rekening_type = rekening_type

    if "rekening_number" in data:
        rekening_number = (data.get("rekening_number") or "").strip()
        if not rekening_number:
            return jsonify({"error": "Rekening number cannot be empty"}), 400

        existing = Rekening.query.filter_by(rekening_number=rekening_number).first()
        if existing and existing.id != rekening.id:
            return jsonify({"error": "Rekening number already exists"}), 400

        rekening.rekening_number = rekening_number

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify({"error": "Rekening number already exists"}), 400

    return jsonify(_to_dict(rekening)), 200


@rekening_bp.route("/<int:rekening_id>", methods=["DELETE"])
def delete_rekening(rekening_id):
    rekening = Rekening.query.get(rekening_id)
    if not rekening:
        return jsonify({"error": "Rekening not found"}), 404

    db.session.delete(rekening)
    db.session.commit()
    return jsonify({"message": "Rekening deleted successfully"}), 200
