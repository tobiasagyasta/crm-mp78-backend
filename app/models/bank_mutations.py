from app.extensions import db
from datetime import datetime
import re

class BankMutation(db.Model):
    __tablename__ = 'bank_mutations'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rekening_number = db.Column(db.String, nullable=False)
    tanggal = db.Column(db.Date, nullable=True)
    transaksi = db.Column(db.String, nullable=True)

    # Parsed fields
    transaction_type = db.Column(db.String, nullable=True)
    transaction_id = db.Column(db.String, nullable=True)
    transaction_amount = db.Column(db.Numeric, nullable=True)
    platform_code = db.Column(db.String, nullable=True)
    platform_name = db.Column(db.String, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def parse_transaction(self):
        """Parse transaction text into components based on known patterns."""
        if not self.transaksi:
            return

        text = self.transaksi.strip()
        tokens = text.split()

        # General extraction
        self.transaction_type = ' '.join(tokens[:3]) if len(tokens) >= 3 else None
        self.transaction_id = tokens[3] if len(tokens) >= 4 else None

        # Extract amount (first float-looking number with at least 4 digits before the decimal)
        amount_match = re.search(r'\b\d{4,}\.\d{2}\b', text)
        self.transaction_amount = float(amount_match.group()) if amount_match else None

        # Gojek
        if "DOMPET ANAK BANGSA" in text.upper():
            self.platform_name = "Gojek"
            outlet_match = re.search(r'\bG\d{9}\b', text)
            self.platform_code = outlet_match.group() if outlet_match else None

        # Grab
        elif "VISIONET" in text.upper():
            self.platform_name = "Grab"
            self.platform_code = None  # Grab has no outlet code

        # Shopee
        elif "AIRPAY" in text.upper():
            try:
                if "MC" in tokens:
                    mc_index = tokens.index("MC")
                    self.platform_name = "Shopee"
                    self.platform_code = tokens[mc_index + 2]
                elif "SF" in tokens:
                    sf_index = tokens.index("SF")
                    self.platform_name = "ShopeeFood"
                    self.platform_code = tokens[sf_index + 2]
                else:
                    self.platform_name = "Shopee"
                    self.platform_code = None
            except (ValueError, IndexError):
                self.platform_code = None

        else:
            self.platform_name = "Unknown"
            self.platform_code = None

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.platform_name} {self.transaction_amount}>"