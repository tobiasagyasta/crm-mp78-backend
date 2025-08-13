from flask import Blueprint, request, jsonify
from app.utils.pkb_mutation import get_minus_manual_entries

test_bp = Blueprint('test', __name__, url_prefix='/test')

@test_bp.route('/minus-entries/<outlet_code>', methods=['GET'])
def minus_entries(outlet_code):
    """
    Test endpoint to return manual entries with 'minus' in description for a given outlet_code.
    """
    from datetime import datetime
    # Parse date params from query string
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None

    entries = get_minus_manual_entries(outlet_code, start_date, end_date)
    def entry_to_dict(entry):
        d = entry.__dict__.copy()
        d.pop('_sa_instance_state', None)
        # Add minus_date as iso string if present
        if hasattr(entry, 'minus_date') and entry.minus_date:
            d['minus_date'] = entry.minus_date.isoformat()
        return d
    return jsonify([entry_to_dict(e) for e in entries])


