import smtplib
import os
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Assuming these are global or passed-in elsewhere
from_email_address = os.environ.get("FROM_EMAIL_ADDRESS")
app_password = os.environ.get("APP_PASSWORD")

def lambda_handler(event, context):
    records = event['Records'][0]
    if not records:
        return {"statusCode": 400, "body": "No records found in the event"}
    order = json.loads(records['Sns']['Message'])
    print('order',order)
    if not order:
        return {"statusCode": 400, "body": "No order data provided"}

    try:
        send_order_email(order)
        return {"statusCode": 200, "body": "Order email sent successfully"}
    except Exception as e:
        return {"statusCode": 500, "body": f"Error sending email: {str(e)}"}

def send_order_email(order):
    sender_email = from_email_address
    receiver_email = order.get("poc_email")
    password = app_password


    subject = f"Your Order Invoice - Order ID {order.get('orderId')}"

    # Build items table rows
    items_html = ""
    for item in order.get("items", []):
        total_price = item["productPrice"] * item["quantity"]
        items_html += f"""
        <tr>
            <td>{item['productId']}</td>
            <td>{item['productName']}</td>
            <td style="text-align:right;">${item['productPrice']:.2f}</td>
            <td style="text-align:right;">{item['quantity']}</td>
            <td style="text-align:right;">${total_price:.2f}</td>
        </tr>
        """

    # HTML Email Body
    html_content = f"""
    <html>
    <body>
        <h2>Order Invoice</h2>
        <table style="width: 100%; margin-bottom: 20px;">
            <tr>
                <td style="vertical-align: top; width: 50%;">
                    <strong>Ship From:</strong><br>{order.get('shipFrom')}<br><br>
                    <strong>Ship To:</strong><br>{order.get('shipTo')}
                </td>
                <td style="vertical-align: top; width: 50%; text-align: right;">
                    <strong>Order Date:</strong> {order.get('orderDate')}<br>
                    <strong>Order ID:</strong> {order.get('orderId')}<br>
                    <strong style="font-size: 18px;">Order Total: ${order.get('orderTotal'):.2f}</strong>
                </td>
            </tr>
        </table>

        <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr style="background-color: #f2f2f2;">
                    <th>Product ID</th>
                    <th>Product Name</th>
                    <th style="text-align:right;">Unit Price</th>
                    <th style="text-align:right;">Quantity</th>
                    <th style="text-align:right;">Total</th>
                </tr>
            </thead>
            <tbody>
                {items_html}
            </tbody>
        </table>

        <p>Thank you for your order!</p>
        <p>Best regards,<br>FarApp Team</p>
        <p>Team FSB</p>
    </body>
    </html>
    """

    # Compose email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print(f"Email sent successfully to {receiver_email}")
        return {"statusCode": 200, "body": "Email sent successfully!"}
    except Exception as e:
        print(f"Failed to send email: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
