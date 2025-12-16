from app.extensions import db
from datetime import datetime
from decimal import Decimal
import re

class VoucherReport(db.Model):
    """
    Represents a voucher redemption report in the database.
    """
    __tablename__ = 'voucher_reports'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_date = db.Column(db.DateTime, nullable=False, index=True)
    order_no = db.Column(db.String(255), unique=True, nullable=False, index=True)
    source = db.Column(db.String(255), nullable=True)
    voucher_id = db.Column(db.String(255), nullable=True)
    voucher_name = db.Column(db.String(255), nullable=True)
    nominal = db.Column(db.Numeric(12, 2), nullable=True)
    merchant_name = db.Column(db.String(255), nullable=True)
    raw_outlet_name = db.Column(db.String(255), nullable=True)
    
    # Foreign Key to link to the Outlet model
    outlet_code = db.Column(db.String(100), db.ForeignKey('outlets.outlet_code'), nullable=True, index=True)
    
    created_at = db.Column(db.DateTime, default=datetime.now)

    # Relationship to the Outlet model (optional but highly recommended)
    # This allows you to easily access the related Outlet object from a VoucherReport instance
    # e.g., my_voucher_report.outlet.area
    outlet = db.relationship('Outlet', backref=db.backref('voucher_reports', lazy='dynamic'))


    def __repr__(self):
        return f"<VoucherReport {self.order_no}>"

    @staticmethod
    def parse_row(row):
        """
        Parses a dictionary-like row from the voucher CSV into a dict
        suitable for creating a VoucherReport instance.

        Assumes the input CSV has the following headers:
        'Order Date', 'Order No', 'Source', 'Voucher ID', 'Voucher Name', 
        'Nominal', 'Merchant Name', 'Outlet Name', 'outlet_code'
        """
        try:
            # --- 1. Parse and clean the nominal value ---
            # Handles formats like " 50,000.00 " by removing spaces, commas.
            nominal_str = row.get('Nominal', '0').strip()
            # Remove all characters that are not a digit or a decimal point
            nominal_cleaned = re.sub(r'[^\d.]', '', nominal_str)
            nominal_amount = Decimal(nominal_cleaned) if nominal_cleaned else Decimal(0)

            # --- 2. Parse the date ---
            order_date_str = row.get('Order Date', '').strip()
            order_date = datetime.strptime(order_date_str, '%Y-%m-%d %H:%M:%S') if order_date_str else None

            # --- 3. Return the structured dictionary ---
            # .strip() is used to remove any accidental leading/trailing whitespace
            return {
                'order_date': order_date,
                'order_no': row.get('Order No', '').strip(),
                'source': row.get('Source', '').strip(),
                'voucher_id': row.get('Voucher ID', '').strip(),
                'voucher_name': row.get('Voucher Name', '').strip(),
                'nominal': nominal_amount,
                'merchant_name': row.get('Merchant Name', '').strip(),
                'raw_outlet_name': row.get('Outlet Name', '').strip(),
                'outlet_code': row.get('outlet_code', '').strip() or None # Use None if the code is an empty string
            }
        except Exception as e:
            return None