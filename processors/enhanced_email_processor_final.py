#!/usr/bin/python3
"""
Enhanced Email Processor - Final Version
Complete email processing with regex matching, data extraction, database insertion, and HTTP API calls
"""

import sys
import re
import json
import logging
import time
import requests
import mysql.connector
from datetime import datetime
from email import message_from_string
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal

# Import custom modules
sys.path.append('/opt')
from database_insertion_module import DatabaseInsertionEngine
from http_api_module import HTTPAPIEngine

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'email_processor',
    'password': 'EmailProc2024!',
    'database': 'email_server',
    'charset': 'utf8mb4'
}

LOG_FILE = '/var/log/email_processor.log'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

class EnhancedEmailProcessor:
    def __init__(self):
        self.db_connection = None
        self.email_id = None
        self.email_data = {}
        self.extracted_data = {}
        self.db_insertion_engine = None
        self.http_api_engine = None
        
    def connect_database(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            self.db_insertion_engine = DatabaseInsertionEngine(self.db_connection)
            self.http_api_engine = HTTPAPIEngine(self.db_connection)
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def close_database(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
            logging.info("Database connection closed")
    
    def parse_email(self, email_content: str) -> Dict[str, Any]:
        """Parse email content and extract metadata"""
        try:
            msg = message_from_string(email_content)
            
            # Extract basic email data
            email_data = {
                'sender': msg.get('From', ''),
                'recipient': msg.get('To', ''),
                'subject': msg.get('Subject', ''),
                'message_id': msg.get('Message-ID', ''),
                'body': '',
                'raw_content': email_content
            }
            
            # Extract body content
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        email_data['body'] = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        break
            else:
                email_data['body'] = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
            
            return email_data
            
        except Exception as e:
            logging.error(f"Email parsing failed: {e}")
            raise
    
    def store_email(self, email_data: Dict[str, Any]) -> int:
        """Store email in database and return email ID"""
        try:
            cursor = self.db_connection.cursor()
            
            # Extract sender name and email
            sender_parts = self.parse_email_address(email_data['sender'])
            recipient_parts = self.parse_email_address(email_data['recipient'])
            
            query = """
                INSERT INTO emails (
                    message_id, sender_email, sender_name, recipient_email, recipient_name,
                    subject, body_text, raw_email, processing_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                email_data['message_id'],
                sender_parts['email'],
                sender_parts['name'],
                recipient_parts['email'],
                recipient_parts['name'],
                email_data['subject'],
                email_data['body'],
                email_data['raw_content'],
                'processing'
            )
            
            cursor.execute(query, values)
            email_id = cursor.lastrowid
            self.db_connection.commit()
            
            logging.info(f"Email stored successfully with ID: {email_id}")
            return email_id
            
        except Exception as e:
            logging.error(f"Email storage failed: {e}")
            raise
        finally:
            cursor.close()
    
    def parse_email_address(self, address: str) -> Dict[str, str]:
        """Parse email address to extract name and email"""
        if '<' in address and '>' in address:
            # Format: "Name <email@domain.com>"
            match = re.match(r'^(.*?)\s*<([^>]+)>$', address.strip())
            if match:
                return {
                    'name': match.group(1).strip().strip('"'),
                    'email': match.group(2).strip()
                }
        
        # Just email address
        return {
            'name': '',
            'email': address.strip()
        }
    
    def get_active_regex_rules(self) -> List[Dict[str, Any]]:
        """Get all active regex rules ordered by priority"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM regex_rules 
                WHERE active = TRUE 
                ORDER BY priority ASC, id ASC
            """
            
            cursor.execute(query)
            rules = cursor.fetchall()
            
            logging.info(f"Retrieved {len(rules)} active regex rules")
            return rules
            
        except Exception as e:
            logging.error(f"Failed to retrieve regex rules: {e}")
            return []
        finally:
            cursor.close()
    
    def check_rule_match(self, rule: Dict[str, Any], email_data: Dict[str, Any]) -> bool:
        """Check if email matches the regex rule patterns"""
        try:
            matches = []
            
            # Check sender pattern
            if rule['sender_pattern']:
                sender_match = bool(re.search(rule['sender_pattern'], email_data['sender'], re.IGNORECASE))
                matches.append(sender_match)
                logging.debug(f"Sender pattern '{rule['sender_pattern']}' match: {sender_match}")
            
            # Check subject pattern
            if rule['subject_pattern']:
                subject_match = bool(re.search(rule['subject_pattern'], email_data['subject'], re.IGNORECASE))
                matches.append(subject_match)
                logging.debug(f"Subject pattern '{rule['subject_pattern']}' match: {subject_match}")
            
            # Check body pattern
            if rule['body_pattern']:
                body_match = bool(re.search(rule['body_pattern'], email_data['body'], re.IGNORECASE))
                matches.append(body_match)
                logging.debug(f"Body pattern '{rule['body_pattern']}' match: {body_match}")
            
            # Apply match logic
            if not matches:
                return False
            
            if rule['match_logic'] == 'AND':
                result = all(matches)
            else:  # OR
                result = any(matches)
            
            logging.info(f"Rule '{rule['name']}' match result: {result}")
            return result
            
        except Exception as e:
            logging.error(f"Rule matching failed for rule '{rule['name']}': {e}")
            return False
    
    def extract_data_from_email(self, rule_id: int, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from email using regex patterns"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get extraction patterns for this rule
            query = """
                SELECT * FROM data_extraction_patterns 
                WHERE rule_id = %s 
                ORDER BY id ASC
            """
            
            cursor.execute(query, (rule_id,))
            patterns = cursor.fetchall()
            
            extracted = {}
            
            for pattern in patterns:
                field_name = pattern['field_name']
                source_field = pattern['source_field']
                regex_pattern = pattern['regex_pattern']
                capture_group = pattern['capture_group']
                data_type = pattern['data_type']
                
                # Get source text
                if source_field == 'sender':
                    source_text = email_data['sender']
                elif source_field == 'subject':
                    source_text = email_data['subject']
                elif source_field == 'body':
                    source_text = email_data['body']
                else:
                    logging.warning(f"Unknown source field: {source_field}")
                    continue
                
                # Apply regex pattern
                match = re.search(regex_pattern, source_text, re.IGNORECASE | re.MULTILINE)
                
                if match:
                    try:
                        raw_value = match.group(capture_group)
                        
                        # Convert data type
                        converted_value = self.convert_data_type(raw_value, data_type)
                        extracted[field_name] = converted_value
                        
                        logging.info(f"Extracted {field_name}: {converted_value}")
                        
                        # Store in database
                        self.store_extracted_data(self.email_id, rule_id, field_name, converted_value, data_type)
                        
                    except IndexError:
                        logging.warning(f"Capture group {capture_group} not found in pattern '{regex_pattern}'")
                    except Exception as e:
                        logging.error(f"Data conversion failed for {field_name}: {e}")
                else:
                    if pattern['required']:
                        logging.warning(f"Required field '{field_name}' not found in {source_field}")
                    else:
                        logging.debug(f"Optional field '{field_name}' not found in {source_field}")
            
            return extracted
            
        except Exception as e:
            logging.error(f"Data extraction failed: {e}")
            return {}
        finally:
            cursor.close()
    
    def convert_data_type(self, value: str, data_type: str) -> Any:
        """Convert extracted string value to specified data type"""
        try:
            if data_type == 'integer':
                # Remove commas and convert to int
                clean_value = re.sub(r'[,\s]', '', value)
                return int(clean_value)
            elif data_type == 'decimal':
                # Remove commas and convert to decimal
                clean_value = re.sub(r'[,\s]', '', value)
                return float(clean_value)
            elif data_type == 'date':
                # Try to parse date (basic implementation)
                from dateutil import parser
                return parser.parse(value).date()
            elif data_type == 'datetime':
                # Try to parse datetime
                from dateutil import parser
                return parser.parse(value)
            else:  # string
                return value.strip()
        except Exception as e:
            logging.warning(f"Data type conversion failed for '{value}' to {data_type}: {e}")
            return value.strip()
    
    def store_extracted_data(self, email_id: int, rule_id: int, field_name: str, field_value: Any, data_type: str):
        """Store extracted data in database"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO extracted_email_data (email_id, rule_id, field_name, field_value, data_type)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                field_value = VALUES(field_value),
                data_type = VALUES(data_type),
                extracted_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(query, (email_id, rule_id, field_name, str(field_value), data_type))
            self.db_connection.commit()
            
        except Exception as e:
            logging.error(f"Failed to store extracted data: {e}")
        finally:
            cursor.close()
    
    def log_rule_execution(self, email_id: int, rule_id: int, execution_type: str, 
                          status: str, execution_time: float, result_data: Any = None, 
                          error_message: str = None):
        """Log rule execution results"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO rule_executions 
                (email_id, rule_id, execution_type, status, execution_time, result_data, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            result_json = json.dumps(result_data) if result_data else None
            
            cursor.execute(query, (
                email_id, rule_id, execution_type, status, execution_time, 
                result_json, error_message
            ))
            self.db_connection.commit()
            
        except Exception as e:
            logging.error(f"Failed to log rule execution: {e}")
        finally:
            cursor.close()
    
    def update_email_status(self, email_id: int, status: str, error_message: str = None):
        """Update email processing status"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                UPDATE emails 
                SET processing_status = %s, processing_error = %s, processed = %s
                WHERE id = %s
            """
            
            processed = 1 if status == 'completed' else 0
            cursor.execute(query, (status, error_message, processed, email_id))
            self.db_connection.commit()
            
        except Exception as e:
            logging.error(f"Failed to update email status: {e}")
        finally:
            cursor.close()
    
    def process_email(self, email_content: str):
        """Main email processing function"""
        start_time = time.time()
        
        try:
            logging.info("Starting complete enhanced email processing")
            
            # Connect to database
            self.connect_database()
            
            # Parse email
            self.email_data = self.parse_email(email_content)
            logging.info(f"Processing email from {self.email_data['sender']} to {self.email_data['recipient']}")
            
            # Store email
            self.email_id = self.store_email(self.email_data)
            
            # Get active regex rules
            rules = self.get_active_regex_rules()
            
            executed_rules = 0
            
            # Process each rule
            for rule in rules:
                rule_start_time = time.time()
                
                try:
                    # Check if email matches rule
                    if self.check_rule_match(rule, self.email_data):
                        logging.info(f"Email matches rule: {rule['name']}")
                        
                        # Extract data if configured
                        if rule['extract_data']:
                            extraction_start = time.time()
                            extracted = self.extract_data_from_email(rule['id'], self.email_data)
                            extraction_time = (time.time() - extraction_start) * 1000
                            
                            self.extracted_data.update(extracted)
                            
                            self.log_rule_execution(
                                self.email_id, rule['id'], 'data_extraction', 
                                'success', extraction_time, extracted
                            )
                        
                        # Insert to database if configured
                        if rule['insert_to_database']:
                            insertion_start = time.time()
                            
                            try:
                                insertion_results = self.db_insertion_engine.process_database_insertions(
                                    rule['id'], self.email_data, self.extracted_data, self.email_id
                                )
                                
                                insertion_time = (time.time() - insertion_start) * 1000
                                
                                if insertion_results['successful_insertions'] > 0:
                                    self.log_rule_execution(
                                        self.email_id, rule['id'], 'database_insertion', 
                                        'success', insertion_time, insertion_results
                                    )
                                    logging.info(f"Database insertion completed: {insertion_results['successful_insertions']} successful")
                                else:
                                    self.log_rule_execution(
                                        self.email_id, rule['id'], 'database_insertion', 
                                        'failed', insertion_time, insertion_results, 
                                        'No successful insertions'
                                    )
                                    logging.warning("Database insertion failed: no successful insertions")
                                
                            except Exception as e:
                                insertion_time = (time.time() - insertion_start) * 1000
                                error_msg = f"Database insertion error: {e}"
                                
                                self.log_rule_execution(
                                    self.email_id, rule['id'], 'database_insertion', 
                                    'failed', insertion_time, None, error_msg
                                )
                                logging.error(error_msg)
                        
                        # Make HTTP calls if configured
                        if rule['make_http_call']:
                            http_start = time.time()
                            
                            try:
                                http_results = self.http_api_engine.process_http_calls(
                                    rule['id'], self.email_data, self.extracted_data, self.email_id
                                )
                                
                                http_time = (time.time() - http_start) * 1000
                                
                                if http_results['successful_calls'] > 0:
                                    self.log_rule_execution(
                                        self.email_id, rule['id'], 'http_call', 
                                        'success', http_time, http_results
                                    )
                                    logging.info(f"HTTP calls completed: {http_results['successful_calls']} successful")
                                else:
                                    self.log_rule_execution(
                                        self.email_id, rule['id'], 'http_call', 
                                        'failed', http_time, http_results, 
                                        'No successful HTTP calls'
                                    )
                                    logging.warning("HTTP calls failed: no successful calls")
                                
                            except Exception as e:
                                http_time = (time.time() - http_start) * 1000
                                error_msg = f"HTTP call error: {e}"
                                
                                self.log_rule_execution(
                                    self.email_id, rule['id'], 'http_call', 
                                    'failed', http_time, None, error_msg
                                )
                                logging.error(error_msg)
                        
                        executed_rules += 1
                        
                    else:
                        logging.debug(f"Email does not match rule: {rule['name']}")
                
                except Exception as e:
                    rule_time = (time.time() - rule_start_time) * 1000
                    error_msg = f"Rule processing failed: {e}"
                    logging.error(error_msg)
                    
                    self.log_rule_execution(
                        self.email_id, rule['id'], 'data_extraction', 
                        'failed', rule_time, None, error_msg
                    )
            
            # Update email status
            self.update_email_status(self.email_id, 'completed')
            
            processing_time = time.time() - start_time
            logging.info(f"Complete enhanced email processing finished. Executed {executed_rules} rules in {processing_time:.3f}s")
            
        except Exception as e:
            error_msg = f"Email processing failed: {e}"
            logging.error(error_msg)
            
            if self.email_id:
                self.update_email_status(self.email_id, 'failed', error_msg)
            
            sys.exit(1)
        
        finally:
            self.close_database()

def main():
    """Main function - read email from stdin and process"""
    try:
        # Read email content from stdin
        email_content = sys.stdin.read()
        
        if not email_content.strip():
            logging.error("No email content received")
            sys.exit(1)
        
        # Process the email
        processor = EnhancedEmailProcessor()
        processor.process_email(email_content)
        
        logging.info("Complete enhanced email processing completed successfully")
        
    except Exception as e:
        logging.error(f"Main processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
