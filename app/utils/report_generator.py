from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from decimal import Decimal

from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopee_reports import ShopeeReport
from app.models.cash_reports import CashReport
from app.models.outlet import Outlet

def generate_daily_report(start_date, end_date, outlet_code):
    # Fetch outlet details
    outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
    if not outlet:
        raise ValueError(f"Outlet with code {outlet_code} not found")

    # Initialize PDF document
    filename = f"daily_report_{outlet_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.pdf"
    doc = SimpleDocTemplate(f"reports/{filename}", pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Add header
    header_style = ParagraphStyle(
        'CustomHeader',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=30
    )
    elements.append(Paragraph(f"Daily Sales Report - {outlet.outlet_name_gojek}", header_style))
    elements.append(Paragraph(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}", styles['Normal']))
    elements.append(Spacer(1, 20))

    # Fetch data from each platform
    gojek_data = GojekReport.query.filter(
        GojekReport.outlet_code == outlet_code,
        GojekReport.waktu_transaksi.between(start_date, end_date)
    ).all()

    grabfood_data = GrabFoodReport.query.filter(
        GrabFoodReport.outlet_code == outlet_code,
        GrabFoodReport.tanggal_dibuat.between(start_date, end_date)
    ).all()

    shopee_data = ShopeeReport.query.filter(
        ShopeeReport.outlet_code == outlet_code,
        ShopeeReport.order_create_time.between(start_date, end_date)
    ).all()

    cash_data = CashReport.query.filter(
        CashReport.outlet_code == outlet_code,
        CashReport.tanggal.between(start_date, end_date)
    ).all()

    # Process Gojek data
    gojek_total = Decimal('0.0')
    if gojek_data:
        elements.append(Paragraph("Gojek Transactions", styles['Heading2']))
        gojek_table_data = [
            ['Date', 'Order Number', 'Gross Sales', 'Commission', 'Net Sales']
        ]
        for record in gojek_data:
            gojek_table_data.append([
                record.waktu_transaksi.strftime('%Y-%m-%d'),
                record.nomor_pesanan,
                f"{record.gross_sales:,.2f}",
                f"{record.total_biaya_komisi:,.2f}",
                f"{record.nett_sales:,.2f}"
            ])
            gojek_total += record.nett_sales

        table = Table(gojek_table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        elements.append(Spacer(1, 20))

    # Process GrabFood data
    grab_total = Decimal('0.0')
    if grabfood_data:
        elements.append(Paragraph("GrabFood Transactions", styles['Heading2']))
        grab_table_data = [
            ['Date', 'Transaction ID', 'Amount', 'Commission', 'Net Sales']
        ]
        for record in grabfood_data:
            net_amount = record.amount - (record.biaya_grab or Decimal('0.0'))
            grab_table_data.append([
                record.tanggal_dibuat.strftime('%Y-%m-%d'),
                record.id_transaksi,
                f"{record.amount:,.2f}",
                f"{record.biaya_grab:,.2f}",
                f"{net_amount:,.2f}"
            ])
            grab_total += net_amount

        table = Table(grab_table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        elements.append(Spacer(1, 20))

    # Process Shopee data
    shopee_total = Decimal('0.0')
    if shopee_data:
        elements.append(Paragraph("Shopee Transactions", styles['Heading2']))
        shopee_table_data = [
            ['Date', 'Order ID', 'Order Amount', 'Commission', 'Net Income']
        ]
        for record in shopee_data:
            shopee_table_data.append([
                record.order_create_time.strftime('%Y-%m-%d'),
                record.order_id,
                f"{record.order_amount:,.2f}",
                f"{record.commission:,.2f}",
                f"{record.net_income:,.2f}"
            ])
            shopee_total += record.net_income or Decimal('0.0')

        table = Table(shopee_table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        elements.append(Spacer(1, 20))

    # Process Cash data
    cash_total = Decimal('0.0')
    if cash_data:
        elements.append(Paragraph("Cash Transactions", styles['Heading2']))
        cash_table_data = [
            ['Date', 'Type', 'Details', 'Total']
        ]
        for record in cash_data:
            amount = record.total if record.type == 'income' else -record.total
            cash_table_data.append([
                record.tanggal.strftime('%Y-%m-%d'),
                record.type.capitalize(),
                record.details,
                f"{amount:,.2f}"
            ])
            cash_total += amount

        table = Table(cash_table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        elements.append(Spacer(1, 20))

    # Add Summary
    total_revenue = gojek_total + grab_total + shopee_total + cash_total
    summary_data = [
        ['Platform', 'Total Revenue'],
        ['Gojek', f"{gojek_total:,.2f}"],
        ['GrabFood', f"{grab_total:,.2f}"],
        ['Shopee', f"{shopee_total:,.2f}"],
        ['Cash', f"{cash_total:,.2f}"],
        ['Total', f"{total_revenue:,.2f}"]
    ]

    elements.append(Paragraph("Summary", styles['Heading2']))
    summary_table = Table(summary_data)
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold')
    ]))
    elements.append(summary_table)

    # Generate PDF
    doc.build(elements)
    return filename