from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import boto3
from bs4 import BeautifulSoup
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
from botocore.exceptions import ClientError
import re
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from dotenv import load_dotenv
from datetime import datetime
import json
import os
from typing import List
import asyncio
import logging
import io
import base64
from pydantic import BaseModel

# Load environment variables from .env file
load_dotenv()

failed_emails = []
succeeded_emails = []
log_succeeded_path = 'succeeded_email_log.txt'
log_failed_path = 'failed_email_log.txt'


# AWS credentials
aws_region = os.getenv('AWS_REGION')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Set up AWS SES client with credentials
ses = boto3.client('ses', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
ses_source_mail = 'loyetgrace@gmail.com'


# my_chosen_templates = 'asdsadas'  # Replace with your chosen template
# # Replace these values with your own
# excel_file_path = 'output_placeholder_names.xlsx'

# aws s3 client with credentials
s3 = boto3.client('s3', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# email-log -details
succeeded_log_key = 'succeeded_email_log.txt'
failed_log_key = 'failed_email_log.txt'


# Configure SES client
ses_client = boto3.client(
    'ses',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)



app = FastAPI()

# Allow all origins in development. Adjust this in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class htmlContent(BaseModel):
    html_content: str

def read_html_file(file):
    try:
        content = file.read().decode('utf-8')
        return content
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail=f"Error reading HTML file - {file.filename}")

def extract_placeholder_names(html_content):
    pattern = re.compile(r'{{(.*?)}}')
    placeholder_names = [match.group(1).strip() for match in re.finditer(pattern, html_content)]
    return placeholder_names

def write_excel(placeholder_names, excel_file_path):
    wb = openpyxl.Workbook()
    sheet = wb.active

    # Adding 'Email' column to the headers
    headers = ['Email'] + placeholder_names + ['Attachment']
    sheet.append(headers)

    # Writing default values for 'Email' and placeholder columns
    default_values = {placeholder_name: '' for placeholder_name in placeholder_names}
    default_values['Email'] = ''

    # Finding the index of the 'Attachment' column
    attachment_column_index = headers.index('Attachment')

    # Adding data validation (dropdown) for the 'Attachment' column
    dv = DataValidation(type="list", formula1='"Y,N"', allow_blank=False)
    dv.add(f'{chr(ord("A") + attachment_column_index)}2:{chr(ord("A") + attachment_column_index)}1048576')  # Assuming a maximum of 1048576 rows
    # Set the default value to "N" directly in the validation
    dv.value = "N"
    sheet.add_data_validation(dv)

    # No need for the formula to set the default value anymore

    # Append rows with default values, including 'Attachment' set to dv.value
    for _ in range(500):  # Assuming a maximum of 1048576 rows
        sheet.append([default_values[col] for col in headers[:-1]] + [dv.value])

    # Save the Excel file
    print(excel_file_path)
    wb.save(excel_file_path)

def create_update_template(template_name, html_content):
    try:
        # Check if the template exists
        existing_templates = ses_client.list_templates()['TemplatesMetadata']
        template_exists = any(template['Name'] == template_name for template in existing_templates)

        if template_exists:
            # If template exists, delete it
            ses_client.delete_template(TemplateName=template_name)
            print(f"Existing template '{template_name}' deleted.")

        # Define SES template
        template = {
            'TemplateName': template_name,
            'HtmlPart': html_content,
            'TextPart': '',
            'SubjectPart': 'Templates'
        }

        # Create SES template
        ses_client.create_template(
            Template=template
        )
        print(f"Template '{template_name}' has been created!")
    except Exception as e:
        print(f'Failed to create/update template {template_name}.', e)

# Make the 'run' function asynchronous
async def run(template_name, html_content):
    try:
        # Write data to Excel file
        excel_file_path = 'Template format.xlsx'
        write_excel(extract_placeholder_names(html_content), excel_file_path)

        # Create or update SES template
        create_update_template(template_name, html_content)
    except Exception as e:
        print(f'Failed to create/update template {template_name}.', e)

def view_template(template_name):
    try:
            existing_template = ses_client.get_template(TemplateName=template_name)['Template']
            if existing_template:
            # If template exists, return existing HTML content for viewing
                existing_html_content = existing_template.get('HtmlPart', '')
            return JSONResponse(content={"message": f"Template '{template_name}' already exists.",
                                         "existing_html_content": existing_html_content})
    except ses_client.exceptions.TemplateDoesNotExistException:
            existing_template = None

        

# def get_template(template_name):
#     try:
#         # Get template information
#         template = ses_client.get_template(TemplateName=template_name)['Template']
#         return template
#     except ses_client.exceptions.TemplateDoesNotExistException:
#         return None

async def send_email_with_attachment(sender, recipient, attachment_file, rendered_template,subject_name):
    attachment_data = None
    attachment_filename = None

    if attachment_file:
        attachment_data = attachment_file.file.read()
        attachment_filename = attachment_file.filename

    try:
        print(f"Recipient: {recipient}")
        print(f"Attachment Filename: {attachment_filename}")
        print(f"Attachment Data Length: {len(attachment_data) if attachment_data else 0}")
        response = ses.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={
                'Data': create_raw_message(sender, recipient, rendered_template, attachment_data, attachment_filename,subject_name)
            }
        )

        print(f"Email sent to {recipient}! Message ID: {response['MessageId']}")
        return response
    except ClientError as e:
        print(f"Error sending email to {recipient}: {e}")
        return None


def create_raw_message(sender, recipient, template_content, attachment_data, attachment_filename,subject_name):
    try:
        message = MIMEMultipart('mixed')
        message['Subject'] = subject_name
        message['From'] = sender
        message['To'] = recipient

        part_html = MIMEText(template_content, 'html')
        message.attach(part_html)

        if attachment_data:
            part_attachment = MIMEApplication(attachment_data)
            part_attachment.add_header('Content-Disposition', 'attachment', filename=attachment_filename)
            message.attach(part_attachment)

        raw_message = message.as_bytes()
        # print(raw_message.decode('utf-8'))

        return raw_message
    except Exception as e:
        print(f"Error creating raw message: {e}")
        raise

def delete_template(template_name, ses_client):
    try:
        # Check if the template exists
        existing_templates = ses_client.list_templates()['TemplatesMetadata']
        template_exists = any(template['Name'] == template_name for template in existing_templates)

        if template_exists:
            # If template exists, delete it
            ses_client.delete_template(TemplateName=template_name)
            print(f"Existing template '{template_name}' deleted.")
        else:
            print(f"Template '{template_name}' does not exist.")
    except Exception as e:
        print(f'Failed to delete template {template_name}.', e)


async def render_template(template_content, template_data):
    for key, value in template_data.items():
        placeholder = '{{' + key + '}}'
        if isinstance(value, (int, float)):
            value = str(int(value)) if value.is_integer() else str(value).rstrip('0').rstrip('.')

        template_content = template_content.replace(placeholder, str(value))

    return template_content

async def log_failed_email(recipient_email, template_name, template_data, error_details):

    log_entry = {
        "timestamp": str(datetime.now()),
        "recipient_email": recipient_email,     
        "subject": template_name,
        "body": json.dumps(template_data),
        "Error_Details": error_details
    }
    print(recipient_email, template_name, template_data, error_details)
    with open('failed_email_log.txt', 'a') as file:
        file.write(json.dumps(log_entry) + '\n')
  
# Use 'async with' for asynchronous file reading
@app.post('/create_template')
async def create_template(
    template_name: str = Form(..., title="Template Name", description="Name of the SES template"),
    html_template: UploadFile = File(..., title="HTML Template", description="HTML template file"),
):
    try:
        content = await html_template.read()
        await run(template_name, content.decode('utf-8'))
    except Exception as e:
        print(f'Error processing template {template_name}.', e)


          
@app.get('/fetch_email_templates')
async def fetch_email_templates():
    loop = asyncio.get_event_loop()

    try:
        # Fetch both regular email templates and SES templates
        response_regular = await loop.run_in_executor(None, ses_client.list_templates)
        response_ses = await loop.run_in_executor(None, ses_client.list_templates)

        # Log the entire response for debugging
        logging.info(f"Response from regular templates: {response_regular}")
        logging.info(f"Response from SES templates: {response_ses}")

        # Extract template names from regular templates
        regular_template_names = [template['Name'] for template in response_regular.get('TemplatesMetadata', [])]

        # Extract template names from SES templates
        ses_template_names = [template['Name'] for template in response_ses.get('Templates', [])]

        # Combine both sets of templates
        all_template_names = set(regular_template_names + ses_template_names)

        logging.info(f"Fetched Email Template Names: {all_template_names}")
        return list(all_template_names)
    except Exception as e:
        logging.error(f"Error fetching email template names: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching email template names: {str(e)}")


@app.get("/fetch_ses_template_names", response_model=List[str])
async def fetch_ses_template_names():
    loop = asyncio.get_event_loop()
    try:
        response = await loop.run_in_executor(None, ses_client.list_templates)
        templates = response.get('Templates', [])
        template_names = [template['TemplateName'] for template in templates]
        logging.info(f"Fetched SES Template Names: {template_names}")
        return template_names
    except Exception as e:
        logging.error(f"Error fetching SES template names: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching SES template names: {str(e)}")

class AttachmentNotFoundException(Exception):
    pass

@app.post('/preview_email')
async def preview_email_route(
    template_name: str = Form(..., title="Template Name", description="Name of the SES template"),
    excel_file: UploadFile = File(..., title="Excel File", description="Excel file"),
):
    try:
        excel_file_content = await excel_file.read()
        template_names = await fetch_email_templates()

        # Validate the selected template exists
        if template_name not in template_names:
            raise ValueError(f"Template '{template_name}' not found.")
        
        # Fetch SES template content
        response = ses.get_template(TemplateName=template_name)
        template_content = response['Template']['HtmlPart']

        # Read details from Excel file
        df = pd.read_excel(io.BytesIO(excel_file_content))

        # Get the first row of the DataFrame
        first_row = df.iloc[0]

        # Provide specific data for the template variables based on the first row
        template_data = {key: first_row[key] for key in df.columns if key != 'Email'}

        # Render the SES template with actual data
        rendered_template = await render_template(template_content, template_data)

        # Return the rendered_template directly as JSON response
        return JSONResponse(content={"rendered_template": rendered_template}, status_code=200)

    except Exception as e:
        return HTTPException(status_code=500, detail=f"Error previewing email: {str(e)}")

@app.post('/send_bulk_emails')
async def send_bulk_emails_route(
    template_name: str = Form(..., title="Template Name", description="Name of the SES template"),
    excel_file: UploadFile = File(..., title="Excel File", description="Excel file"),
    attachment: UploadFile = File(None),  # Make the attachment optional,
    subject_name: str = Form(..., title="Subject", description="Subject section")
):
    try:
        excel_file_content = await excel_file.read()
        template_names = await fetch_email_templates()
        
        # Validate the selected template exists
        if template_name not in template_names:
            raise ValueError(f"Template '{template_name}' not found.")

        # Retrieve the SES template content
        response = ses.get_template(TemplateName=template_name)
        template_content = response['Template']['HtmlPart']

        # Read details from Excel file
        df = pd.read_excel(pd.ExcelFile(excel_file_content))

        async def process_row(row, attachment_file):
            recipient_email = row['Email']

            # Provide specific data for the template variables based on the Excel columns
            template_data = {key: row[key] for key in df.columns if key != 'Email'}

            if recipient_email and not pd.isna(recipient_email):
                try:
                    # Get the value from the 'attachment' column (replace with the actual column name in your Excel file)
                    attachment_column = 'Attachment'
                    attachment_value = row.get(attachment_column, 'N')  # assuming default is 'N' if column is not present

                    # Check if an attachment file is provided

                    print(attachment_file)
                    if attachment_value == 'Y' and not attachment_file: raise AttachmentNotFoundException("No attachment selected")

                    # Render the SES template with actual data
                    rendered_template = await render_template(template_content, template_data)

                    # Send email with rendered template and attachment
                    response = await send_email_with_attachment(
                        ses_source_mail, recipient_email, attachment_file,
                        rendered_template,subject_name
                    )

                    if response:
                        # Log the information to a file
                        
                            log_entry = {
                                "timestamp": str(datetime.now()),
                                "recipient_email": recipient_email,
                                "message_id": response["MessageId"],
                                "subject": subject_name,
                                "body": json.dumps(template_data)
                            }

                            with open('succeeded_email_log.txt', 'a') as file:
                                file.write(json.dumps(log_entry) + '\n')
                    else:
                        await log_failed_email(recipient_email, template_name, template_data, str(e))

                except Exception as e:
                    print(f"Error sending email to {recipient_email}: {e}")
                    # Adding failed email to an array
                    await log_failed_email(recipient_email, template_name, template_data, str(e))

        # Wrap bytes in a BytesIO object before reading Excel content
        df = pd.read_excel(io.BytesIO(excel_file_content))

        # Ensure that attachment_file_content is passed to process_row
        await asyncio.gather(*[process_row(row, attachment) for _, row in df.iterrows()])

        # Upload succeeded email logs and failed email log to S3
        s3.upload_file(log_succeeded_path, 'kjc-email-logs', succeeded_log_key)
        s3.upload_file(log_failed_path, 'kjc-email-logs', failed_log_key)

        return {"message": "Bulk emails sent successfully"}
    except Exception as e:
        print(f'Error sending bulk emails. {e}')
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete("/delete_template/{template_name}")
async def delete_template_endpoint(template_name: str):
    return delete_template(template_name, ses_client)



@app.get('/view-template/{template_name}')
async def view_template_endpoint(template_name: str):
    try:
        result = view_template(template_name)
        return result
    except Exception as e:
        return JSONResponse(content={"message": f"Error: {str(e)}"}, status_code=500)

@app.post("/update-template/{template_name}")
async def update_template_endpoint(template_name: str, html_content: htmlContent):
    try:
        # Check if the template exists
        remain_template = ses_client.get_template(TemplateName=template_name)['Template']
        

        if remain_template:
            # If template exists, update it with the provided HTML content
            remain_template['HtmlPart'] = html_content.html_content
            

            ses_client.update_template(Template=remain_template)
            return JSONResponse(content={"message": f"Template '{template_name}' has been updated!"})
        else:
            # If template does not exist, create it
            new_template = {
                'TemplateName': template_name,
                'HtmlPart': html_content.html_content,
                'TextPart': '',
                'SubjectPart': 'Template'
            }

            # Create SES template
            ses_client.create_template(Template=new_template)
            return JSONResponse(content={"message": f"Template '{template_name}' has been created!"})
    except Exception as e:
        # Return a JSONResponse with an error message
        return JSONResponse(content={"error": f"Failed to create/update template {template_name}. {str(e)}"}, status_code=500)
    
@app.post("/download")
async def download_excel(template_name: str, html_content: dict):
    try:
        # Get existing template from SES
        download_template = ses_client.get_template(TemplateName=template_name)['Template']
        
        if download_template:
            # If the template exists, update it with the provided HTML content

            # Create an in-memory Excel file
            excel_file = io.BytesIO()

            # Write data to the Excel file
            write_excel(extract_placeholder_names(download_template['HtmlPart']), excel_file)

            # Set the cursor at the beginning of the file
            excel_file.seek(0)

            # Return the Excel file as a streaming response
            return StreamingResponse(iter([excel_file.read()]), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={'Content-Disposition': 'attachment; filename=Template_format.xlsx'})
        else:
            raise HTTPException(status_code=404, detail=f"Template {template_name} not found.")
            
    except Exception as e:
        print(f'Failed to find template {template_name}.', e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    

@app.get("/get_SuccessReportData")
async def get_SuccessReportData():
    with open("succeeded_email_log.txt", "r") as file:
        data = file.readlines()
        parsed_data = [json.loads(item.strip()) for item in data]  
    return JSONResponse(content={"data": parsed_data})

# get failed report

@app.get("/get_FailedReportData")
async def get_failedReportData():
    with open("failed_email_log.txt", "r") as file:
        data = file.readlines()
        parsed_data = [json.loads(item.strip()) for item in data]  
    return JSONResponse(content={"data": parsed_data})