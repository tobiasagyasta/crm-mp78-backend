from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from app.models.product import Product
from app.models.outlet import Outlet

# Define association tables for many-to-many relationships
user_brands = db.Table('user_brands',
    db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
    db.Column('product_id', db.Integer, db.ForeignKey('products.id'))
)

user_outlets = db.Table('user_outlets',
    db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
    db.Column('outlet_id', db.Integer, db.ForeignKey('outlets.id'))
)

# models/user.py
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(30), nullable=False)  # Add this line to store the user's role
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Scope relationships using existing Product and Outlet models
    allowed_brands = db.relationship('Product', secondary=user_brands, backref='users')
    allowed_outlets = db.relationship('Outlet', secondary=user_outlets, backref='users')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def get_jwt_identity(self):
        """Return the identity for JWT token"""
        return self.id
    
    def get_jwt_claims(self):
        """Return additional claims for JWT token"""
        return {
            'role': self.role,
            'brand_ids': [brand.id for brand in self.allowed_brands],
            'outlet_ids': [outlet.id for outlet in self.allowed_outlets],
            'is_active': self.is_active
        }
    
    def has_access_to_brand(self, brand_id):
        """Check if user has access to a specific brand"""
        return any(brand.id == brand_id for brand in self.allowed_brands)
    
    def has_access_to_outlet(self, outlet_id):
        """Check if user has access to a specific outlet"""
        return any(outlet.id == outlet_id for outlet in self.allowed_outlets)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role': self.role,
            'brands': [brand.name for brand in self.allowed_brands],
            'outlets': [outlet.outlet_name_gojek for outlet in self.allowed_outlets],
            'is_active': self.is_active
        }