from fpdf import FPDF
from PIL import Image
import os

def create_pdf_from_images(image_folder, output_pdf):
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=False)

    # Get all PNG files in the folder
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
            # Open image and resize it if necessary
            with Image.open(image_path) as img:
                img_w, img_h = img.size

                # Resize the image if it is too large
                max_width, max_height = 1920, 1080  # Resize to a maximum resolution
                if img_w > max_width or img_h > max_height:
                    img.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
                    print(f"Resized image {image_file} to {img.size}")

                # Save the resized image temporarily
                temp_path = os.path.join(image_folder, f"temp_{image_file}")
                img.save(temp_path, format="PNG")

                # Fit the resized image proportionally inside the page
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

                # Remove the temporary resized image
                os.remove(temp_path)

        except Exception as e:
            print(f"Error processing image {image_file}: {e}")
            continue

    # Save the PDF
    try:
        pdf.output(output_pdf)
        print(f"PDF created successfully: {output_pdf}")
    except Exception as e:
        print(f"Error saving PDF: {e}")

# Paths
image_folder = r"c:\Users\Harshini P\Automation\dahsboards\Canara\pngs"
output_pdf = r"c:\Users\Harshini P\Automation\dahsboards\Canara\Dashboard_Report.pdf"

create_pdf_from_images(image_folder, output_pdf)
