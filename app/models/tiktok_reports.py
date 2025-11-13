from app.extensions import db
from app.models.outlet import Outlet
from datetime import datetime

class TiktokReport(db.Model):
    __tablename__ = 'tiktok_reports'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    brand_name = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    outlet_order_id = db.Column(db.String, nullable=False)
    store_name = db.Column(db.String, nullable=True)
    order_time = db.Column(db.DateTime, nullable=False)
    gross_amount = db.Column(db.Numeric, nullable=True)
    net_amount = db.Column(db.Numeric, nullable=True)

    @staticmethod
    def _parse_amount(value: str) -> float:
        if not value or str(value).strip() == "":
            return 0.0
        cleaned = str(value).replace('.', '').replace(',', '').strip()
        return float(cleaned) if cleaned.isdigit() else 0.0

    def __repr__(self):
        return f"<TiktokReport {self.order_id}, {self.order_create_time}>"
    
    @staticmethod
    def parse_tiktok_row(row):
        """
        Parse a TikTok CSV row (list) into a dict suitable for TiktokReport.
        - brand_name and outlet_code: looked up from Outlet via "Kode WEBSHOP dan Tiktok" (last column)
        - outlet_order_id: column 1 (index 1)
        - store_name: column 9 (index 9)
        - order_time: column 4 (index 4, format yyyy-mm-dd)
        - gross_amount: column 14 (index 14)
        - net_amount: column 19 (index 19)
        """
        try:
            tiktok_code = row[-1].strip()
            outlet = None
            if tiktok_code:
                outlet = Outlet.query.filter_by(outlet_code_tiktok_webshop=tiktok_code).first()

            brand_name = outlet.brand if outlet else None
            outlet_code = outlet.outlet_code if outlet else None

            store_name = row[9].strip()
            order_time_str = row[4].strip()
            order_time = datetime.strptime(order_time_str, '%Y-%m-%d')
            gross_amount = TiktokReport._parse_amount(row[14])
            net_amount = TiktokReport._parse_amount(row[19])

            print(row)

            return {
                'brand_name': brand_name,
                'outlet_code': outlet_code,
                'outlet_order_id': row[1].strip(),
                'store_name': store_name,
                'order_time': order_time,
                'gross_amount': gross_amount,
                'net_amount': net_amount
            }
        except Exception as e:
            print(f"[Tiktok Parse Error] Row: {row} | Error: {str(e)}")
            return None
