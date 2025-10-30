from app.extensions import db
from app.models.outlet import Outlet
from datetime import datetime
from sqlalchemy import func

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

    def __repr__(self):
        return f"<TiktokReport {self.order_id}, {self.order_create_time}>"
    
    @staticmethod
    def parse_tiktok_row(row):
        """
        Parse a TikTok CSV row (list) into a dict suitable for TiktokReport.
        - brand_name and outlet_code: looked up from Outlet where outlet_name_gojek == store_name (row[9])
        - outlet_order_id: column 1 (index 1)
        - store_name: column 9 (index 9)
        - order_time: column 5 (index 5, format yyyy-mm-dd)
        - gross_amount: column 14 (index 14)
        - net_amount: column 18 (index 18)
        """
        try:
            store_name = row[9].strip()
            outlet = (Outlet.query
                      .filter(Outlet.outlet_name_gojek.isnot(None))
                      .order_by(func.similarity(Outlet.outlet_name_gojek, store_name).desc())).first()
            if outlet and outlet.outlet_name_gojek:
                sim = db.session.query(func.similarity(Outlet.outlet_name_gojek, store_name)) \
                        .filter(Outlet.id == outlet.id) \
                        .scalar()
                if sim < 0.6:
                    outlet = None
            brand_name = outlet.brand if outlet else None
            outlet_code = outlet.outlet_code if outlet else None

            order_time_str = row[4].strip()
            order_time = datetime.strptime(order_time_str, '%Y-%m-%d')
            gross_amount = float(row[14].replace('.', '').replace(',', '').strip())
            net_amount = float(row[19].replace('.', '').replace(',', '').strip())

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