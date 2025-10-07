from flask import Blueprint, request, jsonify, send_file
from flask_cors import cross_origin
from datetime import datetime
from app.services.excel_export.generator import ExcelReportGenerator
from app.models.outlet import Outlet

export_bp = Blueprint('export', __name__, url_prefix="/export")

@export_bp.route('', methods=['POST', 'OPTIONS'])
@cross_origin(expose_headers=["Content-Disposition"])
def export_reports():
    if request.method == 'OPTIONS':
        return jsonify({'status': 'OK'}), 200

    try:
        data = request.get_json()
        outlet_code = data.get('outlet_code')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        user_role = data.get('user_role')

        if not all([outlet_code, start_date_str, end_date_str]):
            return jsonify({"error": "Missing required parameters"}), 400

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Instantiate the report generator
        generator = ExcelReportGenerator(
            outlet_code=outlet_code,
            start_date=start_date,
            end_date=end_date,
            user_role=user_role
        )
        
        # Generate the report
        excel_file = generator.generate_report()

        # Generate a safe filename
        outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
        safe_outlet_name = outlet.outlet_name_gojek.replace('/', '_').replace('\\', '_').replace(' ', '_')
        filename = f"Report_{safe_outlet_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 404
    except Exception as e:
        # Log the exception for debugging
        print(f"Error during report generation: {e}")
        return jsonify({
            "error": "Report generation failed",
            "details": str(e),
            "type": type(e).__name__
        }), 500