from flask import Blueprint, jsonify, request
from app.extensions import db
from app.models.manual_entry import ManualEntry
from datetime import datetime
from sqlalchemy.exc import IntegrityError

manual_entries_bp = Blueprint('manual_entries', __name__, url_prefix='/manual-entries')

@manual_entries_bp.route('/', methods=['POST'])
def create_entry():
    data = request.get_json()

    try:
        entry = ManualEntry(
            outlet_code=data['outlet_code'],
            entry_type=data['entry_type'],
            amount=data['amount'],
            description=data.get('description'),
            start_date=datetime.strptime(data['start_date'], '%Y-%m-%d').date(),
            end_date=datetime.strptime(data['end_date'], '%Y-%m-%d').date(),
            category_id=data['category_id']
        )

        db.session.add(entry)
        db.session.commit()

        return jsonify(entry.to_dict()), 201

    except KeyError as e:
        return jsonify({'error': f'Missing required field: {str(e)}'}), 400
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except IntegrityError as e:
        db.session.rollback()
        return jsonify({'error': 'Invalid category or entry type'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@manual_entries_bp.route('/', methods=['GET'])
def get_entries():
    outlet_code = request.args.get('outlet_code')
    entry_type = request.args.get('entry_type')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category_id = request.args.get('category_id')

    query = ManualEntry.query

    if outlet_code:
        query = query.filter(ManualEntry.outlet_code == outlet_code)
    if entry_type:
        query = query.filter(ManualEntry.entry_type == entry_type)
    if category_id:
        query = query.filter(ManualEntry.category_id == category_id)
    if start_date:
        query = query.filter(ManualEntry.end_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(ManualEntry.start_date <= datetime.strptime(end_date, '%Y-%m-%d').date())

    # Add joins to get category information
    if entry_type == 'income':
        query = query.join(IncomeCategory, 
                         (ManualEntry.category_id == IncomeCategory.id) & 
                         (ManualEntry.entry_type == 'income'))
    elif entry_type == 'expense':
        query = query.join(ExpenseCategory, 
                         (ManualEntry.category_id == ExpenseCategory.id) & 
                         (ManualEntry.entry_type == 'expense'))

    entries = query.order_by(ManualEntry.start_date.desc()).all()
    return jsonify([entry.to_dict() for entry in entries])

@manual_entries_bp.route('/<int:entry_id>', methods=['GET'])
def get_entry(entry_id):
    entry = ManualEntry.query.get_or_404(entry_id)
    return jsonify(entry.to_dict())

@manual_entries_bp.route('/<int:entry_id>', methods=['PUT'])
def update_entry(entry_id):
    entry = ManualEntry.query.get_or_404(entry_id)
    data = request.get_json()

    try:
        if 'outlet_code' in data:
            entry.outlet_code = data['outlet_code']
        if 'entry_type' in data:
            entry.entry_type = data['entry_type']
        if 'amount' in data:
            entry.amount = data['amount']
        if 'description' in data:
            entry.description = data['description']
        if 'start_date' in data:
            entry.start_date = datetime.strptime(data['start_date'], '%Y-%m-%d').date()
        if 'end_date' in data:
            entry.end_date = datetime.strptime(data['end_date'], '%Y-%m-%d').date()
        if 'category_id' in data:
            entry.category_id = data['category_id']

        db.session.commit()
        return jsonify(entry.to_dict())

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except IntegrityError as e:
        db.session.rollback()
        return jsonify({'error': 'Invalid category or entry type'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@manual_entries_bp.route('/<int:entry_id>', methods=['DELETE'])
def delete_entry(entry_id):
    entry = ManualEntry.query.get_or_404(entry_id)
    
    try:
        db.session.delete(entry)
        db.session.commit()
        return jsonify({'message': 'Entry deleted successfully'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
