from flask import Blueprint, request, jsonify
from app.models.user import User
from app.extensions import db
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required
from app.models.product import Product
from app.models.outlet import Outlet

auth_bp = Blueprint('auth_bp', __name__)

@auth_bp.route('/auth/register', methods=['POST'])
def register():
    data = request.get_json()
    
    if User.query.filter_by(username=data['username']).first():
        return jsonify({'error': 'Username already exists'}), 400
    
    if User.query.filter_by(email=data['email']).first():
        return jsonify({'error': 'Email already exists'}), 400
    
    # Create user with required role
    user = User(
        username=data['username'], 
        email=data['email'],
        role=data.get('role', 'user')  # Default to 'user' if not specified
    )
    user.set_password(data['password'])
    
    # If role is 'management', grant access to all brands and outlets
    if data.get('role') == 'management':
        all_brands = Product.query.all()
        all_outlets = Outlet.query.all()
        user.allowed_brands.extend(all_brands)
        user.allowed_outlets.extend(all_outlets)
    # If role is 'admin', grant access to brand ID 48 and specified outlets
    elif data.get('role') == 'admin' or data.get('role') == 'superadmin':
        # Get brand with ID 48
        brand = Product.query.filter_by(id=48).first()
        if brand:
            user.allowed_brands.append(brand)
            
            # If specific outlet IDs are provided for admin, use those
            if 'admin_outlet_ids' in data and data['admin_outlet_ids']:
                admin_outlets = Outlet.query.filter(Outlet.id.in_(data['admin_outlet_ids'])).all()
                user.allowed_outlets.extend(admin_outlets)
            else:
                # Otherwise, get all outlets associated with this brand
                brand_outlets = Outlet.query.filter_by(brand=brand.name).all()
                user.allowed_outlets.extend(brand_outlets)
    else:
        # Add brand access if provided
        if 'brand_ids' in data and data['brand_ids']:
            brands = Product.query.filter(Product.id.in_(data['brand_ids'])).all()
            user.allowed_brands.extend(brands)
        
        # Add outlet access if provided
        if 'outlet_ids' in data and data['outlet_ids']:
            outlets = Outlet.query.filter(Outlet.id.in_(data['outlet_ids'])).all()
            user.allowed_outlets.extend(outlets)
    
    db.session.add(user)
    db.session.commit()
    
    return jsonify({'message': 'User created successfully'}), 201

@auth_bp.route('/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    user = User.query.filter_by(username=data['username']).first()
    
    if not user or not user.check_password(data['password']):
        return jsonify({'error': 'Invalid credentials'}), 401
    
    if not user.is_active:
        return jsonify({'error': 'Account is inactive'}), 403
    
    # Create access token with additional claims
    additional_claims = user.get_jwt_claims()
    
    # Ensure we're using the user ID as a string for the identity
    access_token = create_access_token(
        identity=str(user.id),  # Convert to string explicitly
        additional_claims=additional_claims
    )

     # Create a custom user response without outlets
    user_data = {
        'id': user.id,
        'username': user.username,
        'email': user.email,
        'role': user.role,
        'brands': [brand.name for brand in user.allowed_brands],
        'is_active': user.is_active
    }
    
    # Only include outlets if the user has fewer than 3 brands
    if len(user.allowed_brands) < 3:
        user_data['outlets'] = [outlet.outlet_code for outlet in user.allowed_outlets]
    
    return jsonify({
        'access_token': access_token,
        'user': user_data
    })

@auth_bp.route('/auth/me', methods=['GET'])
@jwt_required()
def get_current_user():
    # Get the identity from the JWT token
    user_id = get_jwt_identity()
    
    # Make sure we're using a string for the identity
    user = User.query.filter_by(id=user_id).first()
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    return jsonify(user.to_dict())
