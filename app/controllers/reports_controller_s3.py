from flask import Blueprint, request, jsonify, current_app
from app.models.grabfood_reports import GrabFoodReport
from app.extensions import db
from datetime import datetime
import boto3
import pandas as pd
import io
from sqlalchemy.exc import IntegrityError

reports_s3_bp = Blueprint('reports_s3', __name__,url_prefix='/reports-s3')

@reports_s3_bp.route('/upload/grab', methods=['POST'])
def upload_grab_report():
    try:
        # Get parameters from request
        outlet_code = request.form.get('outlet_code')
        brand_name = request.form.get('brand_name')
        file_key = request.form.get('file_key')

        if not all([file_key, outlet_code, brand_name]):
            return jsonify({'error': 'Missing required parameters'}), 400

        # Initialize S3 client using config
        s3_client = boto3.client(
            's3',
            aws_access_key_id=current_app.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=current_app.config['AWS_SECRET_ACCESS_KEY'],
            region_name=current_app.config['AWS_REGION']
        )

        # Download file from S3
        response = s3_client.get_object(
            Bucket=current_app.config['S3_BUCKET'],
            Key=f"reports/grab/{file_key}"
        )
        file_content = response['Body'].read()

        # Read Excel file
        df = pd.read_excel(io.BytesIO(file_content))

        reports = []
        for _, row in df.iterrows():
            try:
                # Convert date columns
                tanggal_dibuat = pd.to_datetime(row['Dibuat pada']) if pd.notna(row['Dibuat pada']) else None
                diperbarui_pada = pd.to_datetime(row['Diperbarui pada']) if pd.notna(row['Diperbarui pada']) else None
                tanggal_transfer = pd.to_datetime(row['Tanggal transfer']) if pd.notna(row['Tanggal transfer']) else None

                report = GrabFoodReport(
                    brand_name="MP78",
                    outlet_code="Test",
                    nama_toko=row.get('Nama toko'),
                    id_toko=row.get('ID toko', ''),
                    diperbarui_pada=diperbarui_pada,
                    tanggal_dibuat=tanggal_dibuat,
                    jenis=row.get('Jenis'),
                    kategori=row.get('Kategori'),
                    subkategori=row.get('Subkategori'),
                    status=row.get('Status'),
                    id_transaksi=row.get('ID transaksi'),
                    id_pesanan_panjang=row.get('ID pesanan (panjang)'),
                    id_pesanan_pendek=row.get('ID pesanan (pendek)'),
                    kode_booking=row.get('Kode booking'),
                    saluran_pesanan=row.get('Saluran pesanan'),
                    jenis_pesanan=row.get('Jenis pesanan'),
                    metode_pembayaran=row.get('Metode pembayaran'),
                    tipe_promo=row.get('Tipe promo'),
                    id_pencairan_dana=row.get('ID pencairan dana'),
                    tanggal_transfer=tanggal_transfer,
                    amount=row.get('Jumlah'),
                    biaya_kemasan=row.get('Biaya kemasan'),
                    diskon_dibiayai_merchant=row.get('Diskon dibiayai merchant'),
                    diskon_ongkos_kirim_dibiayai_merchant=row.get('Diskon ongkos kirim dibiayai merchant'),
                    penjualan_bersih=row.get('Penjualan bersih'),
                    biaya_sukses_pemasaran=row.get('Biaya sukses pemasaran'),
                    komisi_pesanan=row.get('Komisi pesanan'),
                    komisi_lain_grabfood_grabmart=row.get('Komisi lain (GrabFood/GrabMart)'),
                    komisi_grabkitchen=row.get('Komisi GrabKitchen'),
                    komisi_lain_grabkitchen=row.get('Komisi lain (GrabKitchen)'),
                    pajak_pemotongan=row.get('Pajak pemotongan'),
                    total=row.get('Total'),
                    pajak_atas_komisi_grabfood_grabmart=row.get('Pajak atas komisi (GrabFood/GrabMart)'),
                    penyesuaian_iklan=row.get('Penyesuaian iklan'),
                    pajak_atas_total_komisi_grabkitchen=row.get('Pajak atas total komisi (GrabKitchen)'),
                    alasan_pembatalan=row.get('Alasan pembatalan'),
                    dibatalkan_oleh=row.get('Dibatalkan oleh'),
                    alasan_pengembalian_dana=row.get('Alasan pengembalian dana'),
                    deskripsi=row.get('Deskripsi'),
                    kelompok_insiden=row.get('Kelompok insiden'),
                    nama_insiden=row.get('Nama insiden')
                )
                reports.append(report)

            except Exception as e:
                continue

        # Bulk save all reports
        db.session.bulk_save_objects(reports)
        db.session.commit()

        return jsonify({
            'message': 'Reports uploaded successfully',
            'count': len(reports)
        }), 201

    except IntegrityError:
        db.session.rollback()
        return jsonify({'error': 'Duplicate entries found'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500