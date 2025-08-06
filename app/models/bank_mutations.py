from app.extensions import db
from datetime import datetime
from app.models.outlet import Outlet
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

        # Match codes like PDG085
        code_match = re.search(r'([A-Za-z]{3})[-_\s]?(\d{3})', text)
        if not code_match:
            return  # Skip parsing if no valid PKB platform code
        
        normalized_code = f"{code_match.group(1).upper()}-{code_match.group(2)}"
        # print(f"[PKB LOG] Normalized code: {normalized_code}")
        # Safe to proceed
        self.platform_name = "PKB"
        self.platform_code = normalized_code

        # Extract amount (first float-looking number with at least 4 digits before the decimal)
        amount_match = re.search(r'\b\d{4,}\.\d{2}\b', text)
        if amount_match:
            # print(f"[PKB LOG] Found amount: {amount_match.group()}")
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
        Parse a PKB CSV row for 'SOSMED GLOBAL (ALL)'.
        If found, return a dict with 'tanggal' (date) and 'amount' (float). Otherwise, return None.
        """
        try:
            # Check for SOSMED GLOBAL (ALL) first, and only match (AREA) if (ALL) is not present
            if len(row) > 3:
                sosmed_text = row[1]
                if "SOSMED GLOBAL (ALL)" in sosmed_text:
                    print(f"[PKB LOG] Found 'SOSMED GLOBAL (ALL)' in row: {row}")
                    try:
                        tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
                    except Exception as e:
                        print(f"[PKB SOSMED Parse Error] Invalid date: {row[0]} | Error: {str(e)}")
                        return None
                    amount_str = row[3].replace(',', '').replace('"', '').replace('DB', '').replace('CR', '').strip()
                    try:
                        amount = float(amount_str)
                    except Exception as e:
                        print(f"[PKB SOSMED Parse Error] Invalid amount: {row[3]} | Error: {str(e)}")
                        return None
                    if amount > 0:
                        return {'tanggal': tanggal, 'amount': amount, 'type': 'ALL'}
                elif re.search(r'SOSMED GLOBAL \(([^)]+)\)', sosmed_text):
                    area_match = re.search(r'SOSMED GLOBAL \(([^)]+)\)', sosmed_text)
                    area = area_match.group(1).strip().upper()
                    print(f"[PKB LOG] Found 'SOSMED GLOBAL ({area})' in row: {row}")
                    try:
                        tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
                    except Exception as e:
                        print(f"[PKB SOSMED Parse Error] Invalid date: {row[0]} | Error: {str(e)}")
                        return None
                    amount_str = row[3].replace(',', '').replace('"', '').replace('DB', '').replace('CR', '').strip()
                    try:
                        amount = float(amount_str)
                    except Exception as e:
                        print(f"[PKB SOSMED Parse Error] Invalid amount: {row[3]} | Error: {str(e)}")
                        return None
                    if amount > 0:
                        return {'tanggal': tanggal, 'amount': amount, 'type': 'AREA', 'area': area}
            return None
        except Exception as e:
            print(f"[PKB Parse Error] Row: {row} | Error: {str(e)}")
            return None
    @staticmethod
    def parse_pkb_dana_row(row):
        """
        Parse a PKB CSV row for 'DANA' transfer and extract phone number (starts with 8), date, and amount.
        Returns dict with 'tanggal', 'amount', 'phone' if found, else None.
        """
        try:
            # Check if 'DANA' is in column 1
            if len(row) > 3 and 'DANA' in row[1]:
                # Extract phone number (starts with 8, usually after DANA)
                # Match phone number after DANA, starting with 0 or 8
                match = re.search(r'DANA.*?(0?8\d{7,15})', row[1])
                if not match:
                    print(f"[PKB DANA Parse Error] No phone found in row: {row}")
                    return None
                phone = match.group(1)
                # If phone starts with 8, prepend 0
                if phone.startswith('8'):
                    phone = '0' + phone
                # Get date from column 0
                try:
                    tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
                except Exception as e:
                    print(f"[PKB DANA Parse Error] Invalid date: {row[0]} | Error: {str(e)}")
                    return None
                # Get amount from column 3, remove commas, quotes, and 'DB'/'CR'
                amount_str = row[3].replace(',', '').replace('"', '').replace('DB', '').replace('CR', '').strip()
                try:
                    amount = float(amount_str)
                except Exception as e:
                    print(f"[PKB DANA Parse Error] Invalid amount: {row[3]} | Error: {str(e)}")
                    return None
                if amount > 0:
                    return {'tanggal': tanggal, 'amount': amount, 'phone': phone}
            return None
        except Exception as e:
            print(f"[PKB DANA Parse Error] Row: {row} | Error: {str(e)}")
            return None

    @staticmethod
    def parse_pkb_avanger_row(row):
        """
        Parse a PKB CSV row for 'UM AVANGER' or 'GAJI AVANGER'.
        If found, return a dict with 'tanggal' and 'amount'. Otherwise, return None.
        """
        try:
            if len(row) > 3 and ("UM AVANGER" in row[1] or "GAJI AVANGER" in row[1]):
                print(f"[PKB LOG] Found AVANGER in row: {row}")
                avanger_type = "UM AVANGER" if "UM AVANGER" in row[1] else "GAJI AVANGER"
                # Get date from column 0
                try:
                    tanggal = datetime.strptime(row[0].strip().replace("'", ""), '%d/%m/%Y').date()
                except Exception as e:
                    print(f"[PKB AVANGER Parse Error] Invalid date: {row[0]} | Error: {str(e)}")
                    return None
                # Get amount from column 3, remove commas, quotes, and 'DB'/'CR'
                amount_str = row[3].replace(',', '').replace('"', '').replace('DB', '').replace('CR', '').strip()
                try:
                    amount = float(amount_str)
                except Exception as e:
                    print(f"[PKB AVANGER Parse Error] Invalid amount: {row[3]} | Error: {str(e)}")
                    return None
                # Extract description: from 'UM AVANGER' or 'GAJI AVANGER' up to the last number (date) in the string
                desc_match = re.search(r'(UM AVANGER|GAJI AVANGER).*?(\d{1,2}\s*-\s*\d{1,2}\s*[A-Z]{3}\s*\d{2})', row[1])
                if desc_match:
                    description = desc_match.group(0).strip()
                else:
                    # fallback: just use avanger_type
                    description = avanger_type
                if amount > 0:
                    return {'tanggal': tanggal, 'amount': amount, 'type': avanger_type, 'description': description}
            return None
        except Exception as e:
            print(f"[PKB AVANGER Parse Error] Row: {row} | Error: {str(e)}")
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