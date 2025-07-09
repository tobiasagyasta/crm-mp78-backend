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
    @staticmethod
    def parse_pkb_sosmed_row(row):
        """
        Parse a PKB CSV row (list of strings) for the term 'SOSMED GLOBAL'.
        If found, log the row to the console and skip DB insertion.
        """
        try:
            joined_row = ",".join(row)
            if "SOSMED GLOBAL" in joined_row:
                print(f"[PKB LOG] Found 'SOSMED GLOBAL' in row: {row}")
                return None  # Do not insert into DB for now
            return None  # Not a PKB SOSMED GLOBAL row
        except Exception as e:
            print(f"[PKB Parse Error] Row: {row} | Error: {str(e)}")
            return None
    
    @staticmethod
    def parse_pkb_avanger_row(row):
        """
        Parse a PKB CSV row (list of strings) for avangers.
        If found, log the row to the console and skip DB insertion.
        """
        try:
            joined_row = ",".join(row)
            if "UM AVANGER" in joined_row:
                print(f"[PKB LOG] Found 'UM AVANGER' in row: {row}")
                return None  # Do not insert into DB for now
            if "GAJI AVANGER" in joined_row:
                print(f"[PKB LOG] Found 'GAJI AVANGER' in row: {row}")
                return None  # Do not insert into DB for now
            return None  # Not a PKB AVANGER row
        except Exception as e:
            print(f"[PKB Parse Error] Row: {row} | Error: {str(e)}")
            return None

    @staticmethod
    def parse_gojek_row(row):
        """
        Parse a Gojek CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 10 (index 9) is 'BANGSA'.
        - platform_code: from column 6, from first 'G' to end
        - transaction_amount: from column 12
        - platform_name: 'Gojek'
        - tanggal: from column 1
        """
        try:
            # Check if col 10 is 'BANGSA'
            if row[9].strip().upper() != "BANGSA":
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            # Extract platform_code from column 6 (index 5), from first 'G' to end
            col6 = row[5].strip()
            col6 = row[5].strip()
            # Find the first occurrence of 'G' or 'M'
            g_idx = col6.find('G')
            m_idx = col6.find('M')
            if g_idx == -1: g_idx = float('inf')
            if m_idx == -1: m_idx = float('inf')
            start_idx = min(g_idx, m_idx)
            platform_code = col6[start_idx:] if start_idx != float('inf') else col6

            # Transaction amount from column 12 (index 11)
            amount_str = row[11].replace(',', '').replace('"', '').strip()
            transaction_amount = float(amount_str)

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': platform_code,
                'platform_name': 'Gojek',
                'transaksi': ",".join(row)
            }
        except Exception as e:
            print(f"[Gojek Parse Error] Row: {row} | Error: {str(e)}")
            return None
    @staticmethod
    def parse_grab_row(row):
        """
        Parse a Grab CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 8 (index 7) is 'VISIONET'.
        - platform_code: no platform code for Grab (set to None)
        - transaction_amount: from column 11 (index 10)
        - platform_name: 'Grab'
        - tanggal: from column 1 (index 0)
        """
        try:
            # Only parse if col 8 is 'VISIONET'
            if row[7].strip().upper() != 'VISIONET':
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            amount_str = row[10].replace(',', '').replace('"', '').strip()
            transaction_amount = float(amount_str)

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': None,
                'platform_name': 'Grab',
                'transaksi': ",".join(row)
            }
        except Exception as e:
            print(f"[Grab Parse Error] Row: {row} | Error: {str(e)}")
            return None
    @staticmethod
    def parse_shopee_row(row):
        """
        Parse a Shopee CSV row (list of strings) into a dict suitable for BankMutation.
        Only parse if col 10 (index 9) is 'INTERNATION'.
        - platform_code: column 8 (index 7)
        - transaction_amount: from column 12 (index 11)
        - platform_name: Check in column 6 (index 5). If it ends with 'MC', set to 'Shopee', if it ends with 'SF', set to 'ShopeeFood'
        - tanggal: from column 1 (index 0)
        """
        try:
            # Only parse if col 10 (index 9) is 'INTERNATION'
            if row[9].strip().upper() != 'INTERNATION':
                return None

            tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
            platform_code = row[7].strip()  # column 8 (index 7)
            amount_str = row[11].replace(',', '').replace('"', '').replace(".00",'').strip()
            transaction_amount = float(amount_str)

            # Determine platform_name from column 6 (index 5)
            col6 = row[5].strip()
            if col6.endswith('MC'):
                platform_name = 'Shopee'
            elif col6.endswith('SF'):
                platform_name = 'ShopeeFood'
            else:
                platform_name = '-'  # Default/fallback

            return {
                'tanggal': tanggal,
                'transaction_type': None,
                'transaction_id': None,
                'transaction_amount': transaction_amount,
                'platform_code': platform_code,
                'platform_name': platform_name,
                'transaksi': ",".join(row)
            }
        except Exception as e:
            print(f"[Shopee Parse Error] Row: {row} | Error: {str(e)}")
            return None

    @staticmethod
    def detect_platform(row):
        """
        Detects the platform for a given CSV row and returns the corresponding parser function.
        """
        # Gojek: Only parse if col 10 (index 9) is 'BANGSA'
        if row[9].strip().upper() == "BANGSA":
            return BankMutation.parse_gojek_row
        # Grab: Only parse if col 8 (index 7) is 'VISIONET'
        if row[7].strip().upper() == 'VISIONET':
            return BankMutation.parse_grab_row
        # Shopee: Only parse if col 10 (index 9) is 'INTERNATION'
        if row[9].strip().upper() == 'INTERNATION':
            return BankMutation.parse_shopee_row
        # Add more platform checks here as needed
        return None

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.platform_name} {self.transaction_amount}>"