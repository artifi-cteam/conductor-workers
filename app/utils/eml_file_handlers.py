# --- Worker Function ---
import base64
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import mimetypes
import os
import tempfile
import uuid
from app.utils.conductor_logger import log_message
import smtplib


def package_to_eml_worker(task):
    """
    Worker function to package email data into an EML file.
    Expects inputParameters: case_id, email_body, attachments (list),
    sender_email (optional), recipient_email (optional),
                              email_subject (optional)
    Outputs: eml_file_path
    """
    task_id = task.task_id
    log_message(task_id,f"Starting package_to_eml_worker for task: {task_id}")

    try:
        # --- 1. Extract Input Parameters ---
        input_data = task.input_data
        case_id = input_data.get('case_id')
        email_body = input_data.get('email_body', '') # Default to empty string
        attachments_data = input_data.get('attachments', []) # Default to empty list
        sender = input_data.get('sender_email', 'sender@example.com')
        recipient = input_data.get('recipient_email', 'recipient@example.com')
        subject = input_data.get('email_subject', f'Packaged Email for Case {case_id}')

        if not case_id:
            raise ValueError("Missing required input parameter: case_id")
        if not isinstance(attachments_data, list):
             raise ValueError("Attachments parameter must be a list")

        
        log_message(task_id, f"Processing Case ID: {case_id}")
        log_message(task_id,f"Number of attachments received: {len(attachments_data)}")

        # --- 2. Create the Email Message Structure ---
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        # Add Date header? msg['Date'] = formatdate(localtime=True) # Requires: from email.utils import formatdate

        # Attach the body (assuming plain text, use 'html' if body is HTML)
        # You might need another input parameter to specify body type
        msg.attach(MIMEText(email_body, 'plain'))
        log_message(task_id,"Attached email body.")

        # --- 3. Process and Attach Attachments ---
        for attachment in attachments_data:
            filename = attachment.get('filenames')
            # *** Assuming attachment_data is Base64 encoded string ***
            base64_data = attachment.get('attachment_data')

            if not filename or not base64_data:
                logging.warning(f"Skipping attachment due to missing filename or data for task {task_id}")
                continue

            try:
                # Decode Base64 data to bytes
                attachment_bytes = base64.b64decode(base64_data)

                # Guess the MIME type
                ctype, encoding = mimetypes.guess_type(filename)
                if ctype is None or encoding is not None:
                    ctype = 'application/octet-stream' # Default if guess fails
                maintype, subtype = ctype.split('/', 1)

                # Create the attachment part
                part = MIMEBase(maintype, subtype)
                part.set_payload(attachment_bytes)
                encoders.encode_base64(part) # EML standard needs base64 encoding
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
                log_message(task_id,f"Attached file: {filename}")

            except (TypeError, ValueError) as decode_err:
                 logging.error(f"Error decoding or processing attachment '{filename}' for task {task_id}: {decode_err}")
                 # Decide if you want to fail the task or just skip the attachment
                 # raise # Option: re-raise to fail the task
                 continue # Option: skip this attachment and continue
            except Exception as attach_err:
                logging.error(f"Unexpected error attaching file '{filename}' for task {task_id}: {attach_err}")
                raise # Re-raise unexpected errors


        # --- 4. Generate Unique Filename and Save EML ---
        temp_dir = tempfile.gettempdir() # Get system temp directory
        unique_filename = f"{case_id}_{uuid.uuid4()}.eml"
        output_path = os.path.join(temp_dir, unique_filename)

        log_message(task_id,f"Attempting to save EML file to: {output_path}")
        with open(output_path, 'wb') as f: # Use 'wb' (write binary) as msg.as_bytes() returns bytes
            f.write(msg.as_bytes())

        log_message(task_id,f"Successfully created EML file: {output_path} for task {task_id}")

        # --- 4.1. Send Notification Email ---

        smtp_server = "smtp.gmail.com"  # Replace with your SMTP server
        smtp_port = 587  # Common SMTP port for TLS
        smtp_username = "meghanshdev@gmail.com"  # Replace with your email
        smtp_password = "wppc xryq cven lzjl"  # Replace with your email password

        notification_subject = f"Task {task_id} Completed"
        notification_body = f"The EML file for Case ID {case_id} has been successfully created at {output_path}."

        notification_msg = MIMEText(notification_body, 'plain')
        notification_msg['Subject'] = notification_subject
        notification_msg['From'] = smtp_username
        notification_msg['To'] = "meghanshdev@gmail.com"

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # Upgrade the connection to secure
                server.login(smtp_username, smtp_password)
                server.sendmail(smtp_username, recipient, notification_msg.as_string())
                log_message(task_id, f"Notification email sent to {recipient}.")
        except Exception as email_err:
            logging.error(f"Failed to send notification email for task {task_id}: {email_err}")

        # --- 5. Return Success Status and Output ---
        return {
            'status': 'COMPLETED',
            'outputData': {
                'eml_file_path': output_path # Return the path to the created file
            },
            'logs': [f"EML file created at {output_path}"]
        }

    except Exception as e:
        logging.exception(f"Error in package_to_eml_worker for task {task_id}: {e}") # Log traceback
        smtp_server = "smtp.gmail.com"  # Replace with your SMTP server
        smtp_port = 587  # Common SMTP port for TLS
        smtp_username = "meghanshdev@gmail.com"  # Replace with your email
        smtp_password = "wppc xryq cven lzjl"  # Replace with your email password

        notification_subject = f"Task {task_id} Completed"
        notification_body = f"The EML file for Case ID {case_id} has been successfully created at {output_path}."

        notification_msg = MIMEText(notification_body, 'plain')
        notification_msg['Subject'] = notification_subject
        notification_msg['From'] = smtp_username
        notification_msg['To'] = "meghanshdev@gmail.com"

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # Upgrade the connection to secure
                server.login(smtp_username, smtp_password)
                server.sendmail(smtp_username, recipient, notification_msg.as_string())
                log_message(task_id, f"Notification email sent to {recipient}.")
        except Exception as email_err:
            logging.error(f"Failed to send notification email for task {task_id}: {email_err}")

        return {
            'status': 'FAILED',
            'reasonForFailure': 'EML_Packaging_Error', # Custom failure reason
            'logs': [f"Error processing task {task_id}: {str(e)}"]
        }
    
def cleanup_eml_file_worker(task):
    """
    Worker function to delete a specified file.
    Expects inputParameter: file_path_to_delete
    Outputs: cleanup_status message
    """
    task_id = task.task_id
    logging.info(f"Starting cleanup_eml_file_worker for task: {task_id}")

    try:
        # --- 1. Extract Input Parameter ---
        input_data = task.input_data
        file_path = input_data.get('file_path_to_delete')

        if not file_path:
            # If no path is provided, maybe the previous step didn't output one.
            # Consider this success or warning, not usually a failure for cleanup.
            logging.warning(f"No file path provided for cleanup in task {task_id}. Skipping deletion.")
            return {
                'status': 'COMPLETED',
                'outputData': {'cleanup_status': 'No file path provided, cleanup skipped.'},
                'logs': ["No file path provided, cleanup skipped."]
            }

        logging.info(f"Attempting to delete file: {file_path} for task {task_id}")

        # --- 2. Check if File Exists and Delete ---
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Successfully deleted file: {file_path} for task {task_id}")
                status_message = f"File deleted successfully: {file_path}"
                return {
                    'status': 'COMPLETED',
                    'outputData': {'cleanup_status': status_message},
                    'logs': [status_message]
                }
            except OSError as e:
                # Catch potential errors during deletion (e.g., permissions)
                logging.error(f"Error deleting file {file_path} for task {task_id}: {e}")
                # Depending on requirements, you might still return COMPLETED if optional=true
                # Or return FAILED if deletion *must* succeed. Let's choose FAILED here for clarity.
                raise e # Re-raise the exception to let the main handler catch it

        else:
            # File doesn't exist - cleanup is effectively done.
            logging.warning(f"File not found (already deleted?): {file_path} for task {task_id}. Cleanup considered complete.")
            status_message = f"File not found (already deleted?): {file_path}"
            return {
                'status': 'COMPLETED',
                'outputData': {'cleanup_status': status_message},
                'logs': [status_message]
            }

    except Exception as e:
        logging.exception(f"General error in cleanup_eml_file_worker for task {task_id}: {e}")
        return {
            'status': 'FAILED',
            'reasonForFailure': 'File_Cleanup_Error',
            'logs': [f"Error during cleanup task {task_id}: {str(e)}"]
        }