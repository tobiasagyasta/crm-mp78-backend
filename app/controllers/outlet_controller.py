from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.outlet import Outlet
from datetime import datetime

outlet_bp = Blueprint("outlet_bp", __name__, url_prefix="/outlets")

# Create a new outlet
@outlet_bp.route("", methods=["POST"])
def create_outlet():
    data = request.get_json()

    # Check required fields
    required_fields = ['outlet_name_gojek', 'area', 'address']
    missing_fields = [field for field in required_fields if not data.get(field)]
    if missing_fields:
        return jsonify({
            "error": "Missing required fields",
            "missing_fields": missing_fields
        }), 400

    # Generate outlet code from outlet_name_gojek
    outlet_name = data['outlet_name_gojek'].strip()
    if not outlet_name:
        return jsonify({"error": "outlet_name_gojek cannot be empty"}), 400

    # Create outlet code: Take first 3 letters + random 3 digits
    import random
    import string
    
    # Get first 3 letters, convert to uppercase, remove spaces
    name_part = ''.join(c for c in outlet_name if c.isalpha())[:3].upper()
    if len(name_part) < 3:
        return jsonify({"error": "outlet_name_gojek must contain at least 3 letters"}), 400

    # Generate random 3 digits
    num_part = ''.join(random.choices(string.digits, k=3))
    outlet_code = f"{name_part}{num_part}"

    # Check if generated outlet code exists, try up to 5 times
    attempts = 0
    while attempts < 5:
        if not Outlet.query.filter_by(outlet_code=outlet_code).first():
            break
        num_part = ''.join(random.choices(string.digits, k=3))
        outlet_code = f"{name_part}{num_part}"
        attempts += 1
    else:
        return jsonify({"error": "Could not generate unique outlet code. Please try again"}), 500

    try:
        outlet = Outlet(
            outlet_code=outlet_code,
            outlet_name_gojek=data['outlet_name_gojek'],
            outlet_name_grab=data.get('outlet_name_grab'),
            outlet_phone=data.get('outlet_phone'),
            outlet_email=data.get('outlet_email'),
            area=data['area'],
            service_area=data.get('service_area'),
            city_grouping=data.get('city_grouping'),
            store_id_gojek=data.get('store_id_gojek'),
            store_id_grab=data.get('store_id_grab'),
            store_id_shopee=data.get('store_id_shopee'),
            address=data['address'],
            partner_name=data.get('partner_name'),
            partner_phone=data.get('partner_phone'),
            pic_partner_name=data.get('pic_partner_name'),
            pic_phone=data.get('pic_phone'),
            status=data.get('status', 'Active'),
            closing_date=data.get('closing_date'),
            operating_hours=data.get('operating_hours'),
            coordinator_avenger=data.get('coordinator_avenger'),
            brand=data.get('brand'),
            gojek_admin_email=data.get('gojek_admin_email'),
            gojek_admin_password=data.get('gojek_admin_password'),
            grab_admin_email=data.get('grab_admin_email'),
            grab_admin_password=data.get('grab_admin_password'),
            shopee_admin_email=data.get('shopee_admin_email'),
            shopee_admin_password=data.get('shopee_admin_password')
        )

        db.session.add(outlet)
        db.session.commit()
        return jsonify(outlet.to_dict()), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Failed to create outlet: {str(e)}"}), 500

    # Check if outlet code already exists
    if Outlet.query.filter_by(outlet_code=data["outlet_code"]).first():
        return jsonify({"error": "Outlet code already exists"}), 400

    db.session.add(outlet)
    db.session.commit()
    return jsonify(outlet.to_dict()), 201

# Get all outlets
@outlet_bp.route("", methods=["GET"])
def get_outlets():
    # Get query parameters for filtering
    search_term = request.args.get('search', '')
    brand = request.args.get('brand', '')
    missing_store_ids = request.args.get('missing_store_ids', '').lower() == 'true'
    
    # Get pagination parameters
    page = request.args.get('page', None, type=int)
    per_page = request.args.get('per_page', None, type=int)
    
    # Start with base query
    query = Outlet.query.filter(Outlet.status == 'Active').order_by(Outlet.outlet_name_gojek)
    
    # Apply filters if provided
    if search_term:
        query = query.filter(
            (Outlet.outlet_code.ilike(f'%{search_term}%')) |
            (Outlet.outlet_name_gojek.ilike(f'%{search_term}%')) |
            (Outlet.outlet_name_grab.ilike(f'%{search_term}%')) |
            (Outlet.store_id_gojek.ilike(f'%{search_term}%')) |
            (Outlet.store_id_grab.ilike(f'%{search_term}%')) |
            (Outlet.store_id_shopee.ilike(f'%{search_term}%'))
        )
    
    if brand:
        query = query.filter(Outlet.brand == brand)
    
    # Add filter for missing store IDs
    if missing_store_ids:
        query = query.filter(
            (Outlet.store_id_gojek.is_(None)) |
            (Outlet.store_id_gojek == '') |
            (Outlet.store_id_grab.is_(None)) |
            (Outlet.store_id_grab == '') |
            (Outlet.store_id_shopee.is_(None)) |
            (Outlet.store_id_shopee == '')
        )
    
    # Determine if pagination is requested
    if page or per_page:
        # Use default page/per_page if not specified
        page = page or 1
        per_page = per_page or 10
        paginated_outlets = query.paginate(page=page, per_page=per_page, error_out=False)
        
        response = {
            'outlets': [outlet.to_dict() for outlet in paginated_outlets.items],
            'pagination': {
                'total_items': paginated_outlets.total,
                'total_pages': paginated_outlets.pages,
                'current_page': page,
                'per_page': per_page,
                'has_next': paginated_outlets.has_next,
                'has_prev': paginated_outlets.has_prev
            }
        }
    else:
        # Return all results without pagination
        all_outlets = query.all()
        response = {
            'outlets': [outlet.to_dict() for outlet in all_outlets]
        }
    
    return jsonify(response)

# Get a single outlet by ID
@outlet_bp.route("/<int:outlet_id>", methods=["GET"])
def get_outlet(outlet_id):
    outlet = Outlet.query.get(outlet_id)
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404
    return jsonify(outlet.to_dict())

# Get outlet by outlet_code

@outlet_bp.route("/closing/<outlet_code>", methods=["GET"])
def get_closing_date_by_code(outlet_code):
    outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404
    # Convert to string if it's a date/datetime object
    closing_date = (
        outlet.closing_date.isoformat() if hasattr(outlet.closing_date, "isoformat") else outlet.closing_date
    )
    return jsonify({"closing_date": closing_date}), 200
@outlet_bp.route("/code/<outlet_code>", methods=["GET"])
def get_outlet_by_code(outlet_code):
    
    outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
    if not outlet:
        return jsonify({"error": "Outlet not found"}), 404
    return jsonify(outlet.to_dict())

# Update an outlet
@outlet_bp.route("/<int:outlet_id>", methods=["PUT"])
def update_outlet(outlet_id):
    try:
        outlet = Outlet.query.get(outlet_id)
        if not outlet:
            return jsonify({"error": "Outlet not found"}), 404

        data = request.get_json()
        
        # Check if outlet code is being updated and already exists
        if "outlet_code" in data and data["outlet_code"] != outlet.outlet_code:
            if Outlet.query.filter_by(outlet_code=data["outlet_code"]).first():
                return jsonify({"error": "Outlet code already exists"}), 400

        # Update fields
        for field in data:
            if hasattr(outlet, field):
                # Special handling for closing_date
                if field == 'closing_date' and data[field]:
                    try:
                        outlet.closing_date = data[field]
                    except ValueError as e:
                        return jsonify({"error": f"Invalid date format for closing_date: {str(e)}"}), 400
                else:
                    setattr(outlet, field, data[field])
        
        db.session.commit()
        return jsonify(outlet.to_dict())

    except Exception as e:
        db.session.rollback()
        print(f"Error updating outlet {outlet_id}: {str(e)}")
        return jsonify({"error": "Failed to update outlet"}), 500

    outlet.outlet_code = data.get("outlet_code", outlet.outlet_code)
    outlet.outlet_name_gojek = data.get("outlet_name_gojek", outlet.outlet_name_gojek)
    outlet.outlet_name_grab = data.get("outlet_name_grab", outlet.outlet_name_grab)
    outlet.outlet_phone = data.get("outlet_phone", outlet.outlet_phone)
    outlet.outlet_email = data.get("outlet_email", outlet.outlet_email)
    outlet.area = data.get("area", outlet.area)
    outlet.service_area = data.get("service_area", outlet.service_area)
    outlet.city_grouping = data.get("city_grouping", outlet.city_grouping)
    outlet.address = data.get("address", outlet.address)
    outlet.partner_phone = data.get("partner_phone", outlet.partner_phone)
    outlet.pic_partner_name = data.get("pic_partner_name", outlet.pic_partner_name)
    outlet.pic_phone = data.get("pic_phone", outlet.pic_phone)
    outlet.status = data.get("status", outlet.status)
    outlet.closing_date = datetime.fromisoformat(data["closing_date"]) if data.get("closing_date") else outlet.closing_date
    outlet.operating_hours = data.get("operating_hours", outlet.operating_hours)
    outlet.coordinator_avenger = data.get("coordinator_avenger", outlet.coordinator_avenger)
    outlet.store_id_gojek = data.get("store_id_gojek", outlet.store_id_gojek)
    outlet.store_id_grab = data.get("store_id_grab", outlet.store_id_grab)
    outlet.store_id_shopee = data.get("store_id_shopee", outlet.store_id_shopee)
    # Add admin credentials
    outlet.gojek_admin_email = data.get("gojek_admin_email", outlet.gojek_admin_email)
    outlet.gojek_admin_password = data.get("gojek_admin_password", outlet.gojek_admin_password)
    outlet.grab_admin_email = data.get("grab_admin_email", outlet.grab_admin_email)
    outlet.grab_admin_password = data.get("grab_admin_password", outlet.grab_admin_password)
    outlet.shopee_admin_email = data.get("shopee_admin_email", outlet.shopee_admin_email)
    outlet.shopee_admin_password = data.get("shopee_admin_password", outlet.shopee_admin_password)
    
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
