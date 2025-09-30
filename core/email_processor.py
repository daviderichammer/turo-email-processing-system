#!/usr/bin/env python3
"""
Email Processing System for Postfix Integration
This script processes incoming emails, stores them in MySQL, and executes configurable actions.
"""

import sys
import email
import json
import logging
import hashlib
import os
import re
import subprocess
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import decode_header
import mysql.connector
from mysql.connector import Error
import requests

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'email_server',
    'user': 'email_processor',
    'password': 'EmailProc2024!'
}

ATTACHMENT_DIR = '/var/mail/attachments'
LOG_FILE = '/var/log/email_processor.log'

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EmailProcessor:
    def __init__(self):
        self.db_connection = None
        self.ensure_attachment_dir()
    
    def ensure_attachment_dir(self):
        """Ensure attachment directory exists"""
        if not os.path.exists(ATTACHMENT_DIR):
            os.makedirs(ATTACHMENT_DIR, mode=0o755)
            logger.info(f"Created attachment directory: {ATTACHMENT_DIR}")
    
    def connect_db(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            logger.info("Database connection established")
            return True
        except Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("Database connection closed")
    
    def decode_header_value(self, value):
        """Decode email header value"""
        if not value:
            return ""
        
        decoded_parts = decode_header(value)
        decoded_string = ""
        
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                if encoding:
                    decoded_string += part.decode(encoding)
                else:
                    decoded_string += part.decode('utf-8', errors='ignore')
            else:
                decoded_string += part
        
        return decoded_string.strip()
    
    def extract_email_data(self, raw_email):
        """Extract structured data from raw email"""
        try:
            msg = email.message_from_string(raw_email)
            
            # Extract basic headers
            message_id = msg.get('Message-ID', '').strip('<>')
            if not message_id:
                # Generate a unique message ID if missing
                message_id = hashlib.md5(raw_email.encode()).hexdigest()
            
            sender = self.decode_header_value(msg.get('From', ''))
            recipient = self.decode_header_value(msg.get('To', ''))
            subject = self.decode_header_value(msg.get('Subject', ''))
            
            # Extract sender name and email
            sender_email = sender
            sender_name = ""
            if '<' in sender and '>' in sender:
                sender_name = sender.split('<')[0].strip().strip('"')
                sender_email = sender.split('<')[1].split('>')[0].strip()
            
            # Extract recipient name and email
            recipient_email = recipient
            recipient_name = ""
            if '<' in recipient and '>' in recipient:
                recipient_name = recipient.split('<')[0].strip().strip('"')
                recipient_email = recipient.split('<')[1].split('>')[0].strip()
            
            # Extract body content
            body_text = ""
            body_html = ""
            attachments = []
            
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get('Content-Disposition', ''))
                    
                    if content_type == 'text/plain' and 'attachment' not in content_disposition:
                        body_text += part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    elif content_type == 'text/html' and 'attachment' not in content_disposition:
                        body_html += part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    elif 'attachment' in content_disposition:
                        filename = part.get_filename()
                        if filename:
                            attachments.append({
                                'filename': self.decode_header_value(filename),
                                'content_type': content_type,
                                'content': part.get_payload(decode=True)
                            })
            else:
                content_type = msg.get_content_type()
                if content_type == 'text/plain':
                    body_text = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
                elif content_type == 'text/html':
                    body_html = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
            
            return {
                'message_id': message_id,
                'sender_email': sender_email,
                'sender_name': sender_name,
                'recipient_email': recipient_email,
                'recipient_name': recipient_name,
                'subject': subject,
                'body_text': body_text,
                'body_html': body_html,
                'attachments': attachments,
                'raw_email': raw_email
            }
            
        except Exception as e:
            logger.error(f"Error extracting email data: {e}")
            return None
    
    def save_attachments(self, email_id, attachments):
        """Save email attachments to filesystem"""
        saved_attachments = []
        
        for attachment in attachments:
            try:
                # Create safe filename
                safe_filename = re.sub(r'[^\w\-_\.]', '_', attachment['filename'])
                file_path = os.path.join(ATTACHMENT_DIR, f"{email_id}_{safe_filename}")
                
                # Save file
                with open(file_path, 'wb') as f:
                    f.write(attachment['content'])
                
                saved_attachments.append({
                    'filename': attachment['filename'],
                    'content_type': attachment['content_type'],
                    'file_size': len(attachment['content']),
                    'file_path': file_path
                })
                
                logger.info(f"Saved attachment: {file_path}")
                
            except Exception as e:
                logger.error(f"Error saving attachment {attachment['filename']}: {e}")
        
        return saved_attachments
    
    def store_email(self, email_data):
        """Store email in database"""
        try:
            cursor = self.db_connection.cursor()
            
            # Insert email record
            email_query = """
                INSERT INTO emails (
                    message_id, sender_email, sender_name, recipient_email, recipient_name,
                    subject, body_text, body_html, raw_email
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            email_values = (
                email_data['message_id'],
                email_data['sender_email'],
                email_data['sender_name'],
                email_data['recipient_email'],
                email_data['recipient_name'],
                email_data['subject'],
                email_data['body_text'],
                email_data['body_html'],
                email_data['raw_email']
            )
            
            cursor.execute(email_query, email_values)
            email_id = cursor.lastrowid
            
            # Save and store attachments
            if email_data['attachments']:
                saved_attachments = self.save_attachments(email_id, email_data['attachments'])
                
                for attachment in saved_attachments:
                    attachment_query = """
                        INSERT INTO email_attachments (
                            email_id, filename, content_type, file_size, file_path
                        ) VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    attachment_values = (
                        email_id,
                        attachment['filename'],
                        attachment['content_type'],
                        attachment['file_size'],
                        attachment['file_path']
                    )
                    
                    cursor.execute(attachment_query, attachment_values)
            
            self.db_connection.commit()
            logger.info(f"Email stored successfully with ID: {email_id}")
            return email_id
            
        except Error as e:
            logger.error(f"Database error storing email: {e}")
            self.db_connection.rollback()
            return None
        finally:
            cursor.close()
    
    def get_processing_rules(self):
        """Get active processing rules from database"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM processing_rules 
                WHERE is_active = TRUE 
                ORDER BY priority ASC
            """)
            rules = cursor.fetchall()
            cursor.close()
            return rules
        except Error as e:
            logger.error(f"Error fetching processing rules: {e}")
            return []
    
    def evaluate_rule_condition(self, rule, email_data):
        """Evaluate if email matches rule condition"""
        condition_type = rule['condition_type']
        condition_value = rule['condition_value']
        
        try:
            if condition_type == 'sender':
                return condition_value.lower() in email_data['sender_email'].lower()
            elif condition_type == 'subject':
                return condition_value.lower() in email_data['subject'].lower()
            elif condition_type == 'body':
                body_content = (email_data['body_text'] + ' ' + email_data['body_html']).lower()
                return condition_value.lower() in body_content
            elif condition_type == 'attachment':
                if condition_value == 'exists':
                    return len(email_data['attachments']) > 0
                else:
                    return any(condition_value.lower() in att['filename'].lower() 
                             for att in email_data['attachments'])
            elif condition_type == 'custom':
                # For custom conditions, we can evaluate simple expressions
                if condition_value == 'true':
                    return True
                return False
            
        except Exception as e:
            logger.error(f"Error evaluating rule condition: {e}")
            return False
        
        return False
    
    def execute_rule_action(self, rule, email_id, email_data):
        """Execute rule action"""
        action_type = rule['action_type']
        action_config = rule['action_config']
        
        try:
            if isinstance(action_config, str):
                action_config = json.loads(action_config)
        except:
            action_config = {}
        
        start_time = datetime.now()
        result = 'success'
        details = ""
        
        try:
            if action_type == 'webhook':
                # Send webhook
                webhook_url = action_config.get('url')
                if webhook_url:
                    payload = {
                        'email_id': email_id,
                        'sender': email_data['sender_email'],
                        'subject': email_data['subject'],
                        'received_date': datetime.now().isoformat()
                    }
                    response = requests.post(webhook_url, json=payload, timeout=30)
                    details = f"Webhook sent to {webhook_url}, status: {response.status_code}"
                else:
                    result = 'failure'
                    details = "No webhook URL configured"
            
            elif action_type == 'script':
                # Execute script
                script_name = action_config.get('script')
                if script_name:
                    script_path = f"/opt/email_scripts/{script_name}"
                    if os.path.exists(script_path):
                        subprocess.run([script_path, str(email_id)], timeout=60)
                        details = f"Script {script_name} executed"
                    else:
                        result = 'failure'
                        details = f"Script {script_name} not found"
                else:
                    result = 'failure'
                    details = "No script configured"
            
            elif action_type == 'database':
                # Log to processing log (default action)
                details = "Email logged to processing system"
            
            else:
                result = 'skipped'
                details = f"Unknown action type: {action_type}"
        
        except Exception as e:
            result = 'failure'
            details = f"Action execution error: {str(e)}"
            logger.error(f"Error executing rule action: {e}")
        
        # Log the action
        execution_time = (datetime.now() - start_time).total_seconds()
        self.log_processing_action(email_id, rule['id'], rule['rule_name'], result, details, execution_time)
        
        return result
    
    def log_processing_action(self, email_id, rule_id, action_taken, result, details, execution_time):
        """Log processing action to database"""
        try:
            cursor = self.db_connection.cursor()
            log_query = """
                INSERT INTO processing_log (
                    email_id, rule_id, action_taken, result, details, execution_time
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(log_query, (email_id, rule_id, action_taken, result, details, execution_time))
            self.db_connection.commit()
            cursor.close()
            
        except Error as e:
            logger.error(f"Error logging processing action: {e}")
    
    def update_email_status(self, email_id, status, error_message=None):
        """Update email processing status"""
        try:
            cursor = self.db_connection.cursor()
            if error_message:
                cursor.execute("""
                    UPDATE emails 
                    SET processed = TRUE, processing_status = %s, processing_error = %s 
                    WHERE id = %s
                """, (status, error_message, email_id))
            else:
                cursor.execute("""
                    UPDATE emails 
                    SET processed = TRUE, processing_status = %s 
                    WHERE id = %s
                """, (status, email_id))
            
            self.db_connection.commit()
            cursor.close()
            
        except Error as e:
            logger.error(f"Error updating email status: {e}")
    
    def process_email(self, raw_email):
        """Main email processing function"""
        logger.info("Starting email processing")
        
        if not self.connect_db():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Extract email data
            email_data = self.extract_email_data(raw_email)
            if not email_data:
                logger.error("Failed to extract email data")
                return False
            
            logger.info(f"Processing email from {email_data['sender_email']} to {email_data['recipient_email']}")
            
            # Store email in database
            email_id = self.store_email(email_data)
            if not email_id:
                logger.error("Failed to store email in database")
                return False
            
            # Update status to processing
            self.update_email_status(email_id, 'processing')
            
            # Get and execute processing rules
            rules = self.get_processing_rules()
            executed_rules = 0
            
            for rule in rules:
                if self.evaluate_rule_condition(rule, email_data):
                    logger.info(f"Executing rule: {rule['rule_name']}")
                    self.execute_rule_action(rule, email_id, email_data)
                    executed_rules += 1
            
            # Update final status
            self.update_email_status(email_id, 'completed')
            logger.info(f"Email processing completed. Executed {executed_rules} rules.")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing email: {e}")
            if 'email_id' in locals():
                self.update_email_status(email_id, 'failed', str(e))
            return False
        
        finally:
            self.close_db()

def main():
    """Main function - reads email from stdin and processes it"""
    try:
        # Read raw email from stdin (Postfix pipe)
        raw_email = sys.stdin.read()
        
        if not raw_email.strip():
            logger.error("No email data received from stdin")
            sys.exit(1)
        
        # Process the email
        processor = EmailProcessor()
        success = processor.process_email(raw_email)
        
        if success:
            logger.info("Email processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Email processing failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
