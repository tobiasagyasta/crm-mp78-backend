from flask import Blueprint, request, jsonify
from app.models.user import User
from app.extensions import db
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required
from app.models.product import Product
from app.models.outlet import Outlet

auth_bp = Blueprint('auth_bp', __name__)

VALID_ROLES = {'user', 'admin', 'superadmin', 'management'}
ACCESS_MANAGER_ROLES = {'admin', 'superadmin', 'management'}


def _json_error(message, status_code=400):
    return jsonify({'error': message}), status_code


def _parse_id_list(data, *keys):
    for key in keys:
        if key not in data or data.get(key) in (None, ''):
            continue

        raw_value = data.get(key)
        if not isinstance(raw_value, list):
            return None, f'{key} must be a list of IDs'

        ids = []
        for value in raw_value:
            try:
                ids.append(int(value))
            except (TypeError, ValueError):
                return None, f'{key} must contain only numeric IDs'

        return list(dict.fromkeys(ids)), None

    return [], None


def _fetch_by_ids(model, ids, label):
    if not ids:
        return [], None

    rows = model.query.filter(model.id.in_(ids)).all()
    found_ids = {row.id for row in rows}
    missing_ids = sorted(set(ids).difference(found_ids))
    if missing_ids:
        return None, f'Invalid {label} IDs: {missing_ids}'

    rows_by_id = {row.id: row for row in rows}
    return [rows_by_id[id_] for id_ in ids], None


def _outlets_for_products(products):
    if not products:
        return []

    product_names = [product.name for product in products]
    return Outlet.query.filter(Outlet.brand.in_(product_names)).all()


def _serialize_user(user):
    return {
        'id': user.id,
        'username': user.username,
        'email': user.email,
        'role': user.role,
        'product_ids': [product.id for product in user.allowed_brands],
        'products': [product.name for product in user.allowed_brands],
        'outlet_ids': [outlet.id for outlet in user.allowed_outlets],
        'outlets': [outlet.outlet_code for outlet in user.allowed_outlets],
        'is_active': user.is_active,
    }


@auth_bp.route('/auth/register', methods=['POST'])
def register():
    data = request.get_json(silent=True) or {}

    required_fields = ('username', 'email', 'password')
    missing_fields = [field for field in required_fields if not str(data.get(field, '')).strip()]
    if missing_fields:
        return _json_error(f'Missing required fields: {", ".join(missing_fields)}')

    username = str(data['username']).strip()
    email = str(data['email']).strip().lower()
    role = str(data.get('role', 'user')).strip().lower()

    if role not in VALID_ROLES:
        return _json_error(f'Invalid role. Allowed roles: {", ".join(sorted(VALID_ROLES))}')

    if User.query.filter_by(username=username).first():
        return _json_error('Username already exists')

    if User.query.filter_by(email=email).first():
        return _json_error('Email already exists')

    product_ids, error = _parse_id_list(data, 'product_ids', 'brand_ids')
    if error:
        return _json_error(error)

    outlet_ids, error = _parse_id_list(data, 'outlet_ids', 'admin_outlet_ids')
    if error:
        return _json_error(error)

    if product_ids:
        products, error = _fetch_by_ids(Product, product_ids, 'product')
        if error:
            return _json_error(error)
    elif role in {'management', 'superadmin'}:
        products = Product.query.order_by(Product.id.asc()).all()
    else:
        products = []

    if outlet_ids:
        outlets, error = _fetch_by_ids(Outlet, outlet_ids, 'outlet')
        if error:
            return _json_error(error)
    elif products:
        outlets = _outlets_for_products(products)
    elif role in {'management', 'superadmin'}:
        outlets = Outlet.query.all()
    else:
        outlets = []

    user = User(
        username=username,
        email=email,
        role=role
    )
    user.set_password(data['password'])

    user.allowed_brands.extend(products)
    user.allowed_outlets.extend(outlets)

    db.session.add(user)
    db.session.commit()

    return jsonify({
        'message': 'User created successfully',
        'user': _serialize_user(user)
    }), 201


@auth_bp.route('/auth/users/<int:user_id>', methods=['PATCH', 'PUT'])
@jwt_required()
def update_user(user_id):
    data = request.get_json(silent=True) or {}
    user = User.query.get(user_id)

    if not user:
        return _json_error('User not found', 404)

    current_user_id = get_jwt_identity()
    current_user = User.query.get(current_user_id)
    if not current_user or not current_user.is_active:
        return _json_error('Unauthorized', 401)

    is_self_update = current_user.id == user.id
    can_manage_access = current_user.role in ACCESS_MANAGER_ROLES
    access_fields = {'role', 'product_ids', 'brand_ids', 'outlet_ids', 'admin_outlet_ids'}
    requested_access_changes = access_fields.intersection(data.keys())

    if requested_access_changes and not can_manage_access:
        return _json_error('Only admin, superadmin, or management users can change role or access', 403)

    if not is_self_update and not can_manage_access:
        return _json_error('You can only update your own account', 403)

    if 'password' in data:
        password = str(data.get('password') or '').strip()
        if not password:
            return _json_error('password cannot be empty')
        user.set_password(password)

    if 'role' in data:
        role = str(data.get('role') or '').strip().lower()
        if role not in VALID_ROLES:
            return _json_error(f'Invalid role. Allowed roles: {", ".join(sorted(VALID_ROLES))}')
        user.role = role

    product_ids, error = _parse_id_list(data, 'product_ids', 'brand_ids')
    if error:
        return _json_error(error)

    outlet_ids, error = _parse_id_list(data, 'outlet_ids', 'admin_outlet_ids')
    if error:
        return _json_error(error)

    if 'product_ids' in data or 'brand_ids' in data:
        products, error = _fetch_by_ids(Product, product_ids, 'product')
        if error:
            return _json_error(error)

        user.allowed_brands = products

        outlets_by_id = {outlet.id: outlet for outlet in _outlets_for_products(products)}
        if outlet_ids:
            explicit_outlets, error = _fetch_by_ids(Outlet, outlet_ids, 'outlet')
            if error:
                return _json_error(error)
            outlets_by_id.update({outlet.id: outlet for outlet in explicit_outlets})

        user.allowed_outlets = list(outlets_by_id.values())
    elif 'outlet_ids' in data or 'admin_outlet_ids' in data:
        outlets, error = _fetch_by_ids(Outlet, outlet_ids, 'outlet')
        if error:
            return _json_error(error)
        user.allowed_outlets = outlets

    db.session.commit()

    return jsonify({
        'message': 'User updated successfully',
        'user': _serialize_user(user)
    })

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
