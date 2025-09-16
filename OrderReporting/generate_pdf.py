import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
)
from reportlab.lib.units import inch

import io
import os
import boto3
from boto3.session import Session


bucket_name = os.environ.get('REPORTS_BUCKET_NAME')
AWS_REGION = os.environ.get('AWS_REGION')


s3_client = boto3.client('s3', region_name=AWS_REGION)


def make_pdf(report):
    print('===> report data', report)

    date = report['date'].strftime('%Y-%m-%d')
    total_orders = report['total_orders']
    total_sales = report['total_sales']

    product_counts = report['product_counts']

    product_wise_sales = report['product_wise_sales']

    transaction_types = report['transaction_types']


    product_ids = list(product_counts.keys())
    counts = list(product_counts.values())

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(range(len(product_ids)), counts, color='skyblue')

    # Display product ids vertically inside bars
    for bar, pid in zip(bars, product_ids):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width()/2, height/2,
            str(pid),
            ha='center', va='center',
            rotation=90,
            color='black',
            fontsize=8,
            fontweight='bold'
        )

    ax.set_ylabel('Units Sold')
    ax.set_xticks([])  # remove default ticks
    plt.tight_layout()

    # Save plot to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='PNG')
    plt.close(fig)
    buf.seek(0)

    # Step 2: Create PDF with reportlab

    pdf_file = f'{date}_sales_summary.pdf'
    doc = SimpleDocTemplate(buf, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=18)
    styles = getSampleStyleSheet()

    elements = []

    # Header with date and orders (left) and total revenue (right)

    header_data = [
        [
            Paragraph(f'<b>Date: {date}</b><br/>Orders Placed: {total_orders}', styles['Normal']),
            Paragraph(f'<b>Total Revenue</b>: {total_sales:.2f} USD', styles['Heading2'])
        ]
    ]

    header_table = Table(header_data, colWidths=[270, 270])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
    ]))
    elements.append(header_table)
    elements.append(Spacer(1, 12))

    # Section: Units Sold
    elements.append(Paragraph("<b>Units Sold</b>", styles['Heading2']))
    elements.append(Spacer(1, 12))

    # Insert bar chart image
    img = Image(buf, width=6.5*inch, height=3*inch)
    elements.append(img)
    elements.append(Spacer(1, 24))

    # Section: Product Wise Sales
    elements.append(Paragraph("<b>Product Wise Sales</b>", styles['Heading2']))
    elements.append(Spacer(1, 12))

    # Create table data for product wise sales
    product_sales_data = [["Product ID", "Units Sold", "Revenue (USD)"]]
    for pid in product_counts:
        product_sales_data.append([str(pid), str(product_counts[pid]), f"{product_wise_sales[pid]:.2f}"])

    product_sales_table = Table(product_sales_data, hAlign='LEFT')
    product_sales_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
        ('ALIGN',(1,1),(-1,-1),'RIGHT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.black),
    ]))
    elements.append(product_sales_table)
    elements.append(Spacer(1, 24))

    # Transaction types table
    elements.append(Paragraph("<b>Transaction Summary</b>", styles['Heading2']))
    elements.append(Spacer(1, 12))

    transaction_data = [
        ["Transaction Type", "Total Orders", "Total Revenue (USD)"],
        ['Credit Card', transaction_types['cc'][0], f"{transaction_types['cc'][1]:.2f}"],
        ['Debit Card', transaction_types['db'][0], f"{transaction_types['db'][1]:.2f}"],
        ['Gift Card', transaction_types['gc'][0], f"{transaction_types['gc'][1]:.2f}"],
    ]

    transaction_table = Table(transaction_data, hAlign='LEFT')
    transaction_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
        ('ALIGN',(1,1),(-1,-1),'RIGHT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.black),
    ]))
    elements.append(transaction_table)

    # Build PDF
    doc.build(elements)

    print(f"PDF saved as {pdf_file}")
    buf.seek(0)
    save_pdf_s3(buf, pdf_file)


def save_pdf_s3(document, file_name):
    try:
        print('bucketname', bucket_name, 'type', type(document))
        s3_client.upload_fileobj(document, bucket_name, str(file_name) )
        print('Saved report summary successfully to s3 bucket')

    except Exception as e:
        print('Error saving pdf file to s3', e)
        return
    

if __name__ == '__main__':
    make_pdf({})