import io
import os
import json
import boto3
from fpdf import FPDF

s3 = boto3.client('s3')
bucket = os.environ['INVOICE_BUCKET_NAME']  # Set this in Lambda environment variables

class InvoicePDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 14)
        self.cell(0, 10, "Invoice", ln=True, align="C")
        self.ln(5)

def lambda_handler(event, context):
    try:
        records = event['Records'][0]
        order = json.loads(records['Sns']['Message'])  # Parse from SNS message string
        print('Order details:', order)
        
        pdf = generate_invoice_pdf(order)
        key = f"invoices/{order['orderId']}_invoice.pdf"
        s3.put_object(Bucket=bucket, Key=key, Body=pdf.getvalue())
        return {'pdfKey': key}
    
    except Exception as e:
        print(f"Error generating or uploading invoice: {e}")
        raise

def generate_invoice_pdf(order):
    pdf = InvoicePDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # === Shipping Info ===
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Ship From: {order['shipFrom']}", ln=True)
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, f"Ship To: {order['shipTo']}", ln=True)
    
    # === Order Info ===
    pdf.ln(5)
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, f"Order ID: {order['orderId']}", ln=True)
    pdf.cell(0, 8, f"Order Date: {order['orderDate']}", ln=True)
    
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Order Total: ${order['orderTotal']:.2f}", ln=True)

    # === Items Table ===
    pdf.ln(10)
    pdf.set_font("Arial", "B", 11)
    col_widths = [35, 65, 25, 20, 30]
    headers = ["Product ID", "Product Name", "Unit Price", "Qty", "Total"]

    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 10, header, border=1)
    pdf.ln()

    pdf.set_font("Arial", "", 10)
    for item in order['items']:
        total_price = item['productPrice'] * item['quantity']
        row = [
            item['productId'],
            item['productName'],
            f"${item['productPrice']:.2f}",
            str(item['quantity']),
            f"${total_price:.2f}"
        ]
        for i, data in enumerate(row):
            pdf.cell(col_widths[i], 8, data, border=1)
        pdf.ln()

    # === Total row ===
    pdf.set_font("Arial", "B", 11)
    pdf.cell(sum(col_widths[:-2]), 8, "", border=0)
    pdf.cell(col_widths[-2], 8, "Total:", border=1)
    pdf.cell(col_widths[-1], 8, f"${order['orderTotal']:.2f}", border=1)

    # Get PDF as bytes string
    pdf_bytes = pdf.output(dest='S').encode('latin1')  # encoding is important

    buffer = io.BytesIO(pdf_bytes)
    buffer.seek(0)
    return buffer
