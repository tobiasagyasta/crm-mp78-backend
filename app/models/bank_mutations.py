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
        """Parse a full CSV transaction row string into structured fields, per column."""

        if not self.transaksi:
            return

        try:
            # Split the transaction string into columns
            fields = [f.strip() for f in self.transaksi.split(',')]

            # Parse 'tanggal' from Column 0 (index 0) — remove apostrophe and any time portion
            raw_date = fields[0].lstrip("'").split()[0]  # e.g. '27/02/2025 10:00' -> '27/02/2025'
            try:
                self.tanggal = datetime.strptime(raw_date, "%d/%m/%Y").date()
            except ValueError:
                self.tanggal = None

            # Transaction type is combined from columns 1, 2, 3 (index 1-3)
            if len(fields) >= 4:
                self.transaction_type = ' '.join(fields[1:4])

            # Transaction ID from column 4 (index 4)
            self.transaction_id = fields[4] if len(fields) > 4 else None

            # Detect platform & platform_code and amount accordingly
            joined_text_upper = ' '.join(fields).upper()

            if "DOMPET ANAK BANGSA" in joined_text_upper:
                self.platform_name = "Gojek"
                # Platform code inside column 6 (index 5)
                platform_code_field = fields[5] if len(fields) > 5 else ''
                match = re.search(r'G\d{9}', platform_code_field)
                self.platform_code = match.group() if match else None

                # Amount is in column 12 (index 11)
                try:
                    amount_str = fields[11].replace(',', '')
                    self.transaction_amount = float(amount_str)
                except (IndexError, ValueError):
                    self.transaction_amount = None

            elif "VISIONET INTERNASI" in joined_text_upper:
                self.platform_name = "Grab"
                self.platform_code = None
                # Amount in column 12 (index 11)
                try:
                    amount_str = fields[11].replace(',', '')
                    self.transaction_amount = float(amount_str)
                except (IndexError, ValueError):
                    self.transaction_amount = None

            elif "AIRPAY INTERNATION" in joined_text_upper:
                # Shopee/ShopeeFood
                if len(fields) > 5 and fields[5] == 'SF':
                    self.platform_name = "ShopeeFood"
                elif len(fields) > 5 and fields[5] == 'MC':
                    self.platform_name = "ShopeePay"
                else:
                    self.platform_name = "Shopee"
                self.platform_code = fields[6] if len(fields) > 6 else None

                # Amount is in column 6 (index 5) but may have trailing letters (SF or MC)
                amount_str = re.sub(r'[A-Z]+$', '', fields[5]) if len(fields) > 5 else None
                try:
                    self.transaction_amount = float(amount_str) if amount_str else None
                except ValueError:
                    self.transaction_amount = None

            else:
                self.platform_name = "Unknown"
                self.platform_code = None
                # Try fallback amount from column 12 (index 11)
                try:
                    amount_str = fields[11].replace(',', '')
                    self.transaction_amount = float(amount_str)
                except (IndexError, ValueError):
                    self.transaction_amount = None

        except Exception as e:
            print(f"[Parse Error] {str(e)}")


    

    def parse_pkb_transaction(self):
        """
        Parse transaction text specifically for PKB platform.
        Format must contain a valid platform code like 'PDG-085'.
        Ignores entries that do not match the required format.
        """
        if not self.transaksi:
            return

        text = self.transaksi.strip()

        # Must contain valid PKB platform code like PDG-085
        code_match = re.search(r'\b[A-Z]{3}-\d{3}\b', text)
        if not code_match:
            return  # Skip parsing if no valid PKB platform code

        # Safe to proceed
        self.platform_name = "PKB"
        self.platform_code = code_match.group()

        # Extract amount (first float-looking number with at least 4 digits before the decimal)
        amount_match = re.search(r'\b\d{4,}\.\d{2}\b', text)
        if amount_match:
            self.transaction_amount = float(amount_match.group())

        # Set generic transaction_type if needed
        self.transaction_type = "TRSF E-BANKING DB"
        self.transaction_id = None

        # Extract transaction description (everything after platform code)
        after_code = text.split(self.platform_code, 1)[-1].strip()
        self.transaksi = after_code  # override with cleaned-up description

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.platform_name} {self.transaction_amount}>"