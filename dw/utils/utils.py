import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv
load_dotenv()


def send_email(email_reciver, config_id, stage, error_message):
    """
    Hàm gửi email thông báo lỗi
    Args:
        email (str): Email người nhận
        config_id (int): ID của config
        stage (str): Stage đang chạy
        error_message (str): Thông báo lỗi
    Returns: None
    """
    from_email = os.getenv("EMAIL")
    to_email = email_reciver
    subject = f"Error in config {config_id} at stage {stage}"
    body = f"An error occurred:\n\n{error_message}"

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Send the email
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, os.getenv("PASSWORD_EMAIL"))
        server.send_message(msg)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")
