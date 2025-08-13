from app.models.manual_entry import ManualEntry
from datetime import timedelta
from app.extensions import db

def get_minus_manual_entries(outlet_code_param, start_date=None, end_date=None):
    """
    Returns all manual_entries for the given outlet_code_param where brand_name is 'Pukis & Martabak Kota Baru',
    description contains 'minus' (case-insensitive), and the date after MINUS (in Indonesian months) is within the date range.
    start_date and end_date should be date objects or None.
    """
    import re
    from datetime import datetime
    indo_months = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MEI': 5, 'JUN': 6, 'JUL': 7, 'AGU': 8, 'SEP': 9, 'OKT': 10, 'NOV': 11, 'DES': 12
    }
    def parse_minus_date(desc):
        match = re.search(r'MINUS(\d{1,2})([A-Z]{3})', desc.upper())
        if match:
            day = int(match.group(1))
            month_str = match.group(2)
            month = indo_months.get(month_str)
            if month:
                year = datetime.now().year
                try:
                    return datetime(year, month, day).date()
                except Exception:
                    return None
        return None

    entries = ManualEntry.query.filter(
        ManualEntry.brand_name == 'Pukis & Martabak Kota Baru',
        ManualEntry.description.ilike('%minus%'),
        ManualEntry.outlet_code == outlet_code_param
    ).all()
    filtered = []
    for e in entries:
        minus_date = parse_minus_date(e.description)
        if minus_date:
            if start_date and minus_date < start_date:
                continue
            if end_date and minus_date > end_date:
                continue
        else:
            continue
        e.minus_date = minus_date + timedelta(days=1)
        filtered.append(e)
    return filtered
