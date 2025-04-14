import os
import pickle
import base64
from fpdf import FPDF
from PIL import Image
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Gmail API Configuration
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
TOKEN_PATH = os.getenv("TOKEN_PATH")
CLIENT_SECRET_FILE = os.getenv("CLIENT_SECRET_FILE")

# === Gmail Authentication ===
def authenticate_gmail():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "wb") as token:
            pickle.dump(creds, token)
    return creds

# === Send Email with PDF ===
def send_email(pdf_filename):
    recipient = "harshiniprakasam@gmail.com"
    subject = "Monthly Fraud Management Report"
    body = (
        "Dear Team,\n\n"
        "This is an auto-generated email with a PDF attachment containing the CANARA lreport.\n\n"
        "Best regards,\nHarshini"
    )

    creds = authenticate_gmail()
    service = build("gmail", "v1", credentials=creds)

    message = MIMEMultipart()
    message["to"] = recipient
    message["subject"] = subject
    message.attach(MIMEText(body, "plain"))

    with open(pdf_filename, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(pdf_filename)}")
        message.attach(part)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    try:
        service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        print(f"Email sent successfully to {recipient} with {pdf_filename}")
    except HttpError as error:
        print(f" An error occurred while sending email: {error}")

# === Create PDF from PNGs ===
def create_pdf_from_images(image_folder, output_pdf):
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=False)

    images = [f for f in os.listdir(image_folder) if f.lower().endswith('.png')]
    images.sort()

    if not images:
        print("No PNG images found in the folder.")
        return

    print(f"Found {len(images)} images in the folder.")
    page_w, page_h = 297, 210  # A4 landscape in mm

    for i, image_file in enumerate(images):
        image_path = os.path.join(image_folder, image_file)
        print(f"Processing image {i + 1}/{len(images)}: {image_file}")

        try:
            with Image.open(image_path) as img:
                img_w, img_h = img.size
                max_width, max_height = 1920, 1080
                if img_w > max_width or img_h > max_height:
                    img.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
                    print(f"Resized image {image_file} to {img.size}")

                temp_path = os.path.join(image_folder, f"temp_{image_file}")
                img.save(temp_path, format="PNG")

                img_aspect = img_w / img_h
                page_aspect = page_w / page_h

                if img_aspect > page_aspect:
                    display_w = page_w
                    display_h = page_w / img_aspect
                else:
                    display_h = page_h
                    display_w = page_h * img_aspect

                x = (page_w - display_w) / 2
                y = (page_h - display_h) / 2

                pdf.add_page()
                pdf.image(temp_path, x=x, y=y, w=display_w, h=display_h)
                os.remove(temp_path)

        except Exception as e:
            print(f"Error processing image {image_file}: {e}")
            continue

    try:
        pdf.output(output_pdf)
        print(f"PDF created successfully: {output_pdf}")
        send_email(output_pdf)  # <-- Call email function after PDF is saved
    except Exception as e:
        print(f"Error saving PDF: {e}")

# === Run Script ===
image_folder = r"c:\Users\Harshini P\Automation\dahsboards\Canara\pngs"
output_pdf = r"c:\Users\Harshini P\Automation\dahsboards\Canara\Dashboard_Report.pdf"

create_pdf_from_images(image_folder, output_pdf)
