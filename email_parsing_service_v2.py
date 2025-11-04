"""
Email Parsing Service - Standalone (v2 - Best Match Only)
Processes already-ingested emails and populates parsed_data table
Saves ONLY the best match per email (matching original behavior)
"""
import mysql.connector
import time
import logging
import sys
import re
import json
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'SecureRootPass123!',
    'database': 'email_server'
}

LOG_FILE = '/root/email-system/logs/parsing_service.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class EmailParsingService:
    def __init__(self):
        self.db = None
        self.templates = []
        
    def connect_db(self):
        try:
            self.db = mysql.connector.connect(**DB_CONFIG)
            logger.info('Database connected')
            return True
        except Exception as e:
            logger.error(f'DB connection failed: {e}')
            return False
    
    def load_templates(self):
        """Load parsing templates from database"""
        try:
            cursor = self.db.cursor(dictionary=True)
            cursor.execute('''
                SELECT id, category_name, template_name, field_extractions
                FROM parsing_rule_templates
                WHERE is_active = 1
                ORDER BY id ASC
            ''')
            self.templates = cursor.fetchall()
            cursor.close()
            logger.info(f'Loaded {len(self.templates)} templates')
            return True
        except Exception as e:
            logger.error(f'Failed to load templates: {e}')
            return False
    
    def parse_with_template(self, template, subject, body_text, body_html):
        """Parse email using a template"""
        try:
            field_extractions = json.loads(template['field_extractions'])
            
            # Handle both dict and list formats
            if isinstance(field_extractions, list):
                logger.debug(f'Template {template["id"]} has list format, skipping')
                return None
            
            if not isinstance(field_extractions, dict):
                logger.debug(f'Template {template["id"]} has invalid format')
                return None
            
            parsed_fields = {}
            matches = 0
            total_fields = len(field_extractions)
            
            for field_name, field_config in field_extractions.items():
                try:
                    # Handle different config formats
                    if isinstance(field_config, dict):
                        pattern = field_config.get('pattern', '')
                        source = field_config.get('source', 'body')
                        group = field_config.get('group', 1)
                    elif isinstance(field_config, list):
                        # Old format: list of regex patterns
                        pattern = field_config[0] if field_config else ''
                        source = 'body'
                        group = 1
                    else:
                        continue
                    
                    # Select source text
                    if source == 'subject':
                        text = subject or ''
                    elif source == 'html':
                        text = body_html or ''
                    else:
                        text = body_text or ''
                    
                    # Try to match
                    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    if match:
                        try:
                            if group > 0 and len(match.groups()) >= group:
                                parsed_fields[field_name] = match.group(group).strip()
                            else:
                                parsed_fields[field_name] = match.group(0).strip()
                            matches += 1
                        except:
                            pass
                            
                except Exception as e:
                    logger.debug(f'Error extracting field {field_name}: {e}')
                    continue
            
            if matches == 0:
                return None
            
            # Calculate confidence and status
            confidence = matches / total_fields if total_fields > 0 else 0.0
            if matches == total_fields:
                status = 'success'
            elif matches > 0:
                status = 'partial'
            else:
                status = 'failed'
            
            return {
                'template_id': template['id'],
                'category_name': template['category_name'],
                'parsed_fields': parsed_fields,
                'confidence_score': confidence,
                'parsing_status': status,
                'matches': matches,
                'total_fields': total_fields
            }
            
        except Exception as e:
            logger.error(f'Error parsing with template {template["id"]}: {e}')
            return None
    
    def save_parsed_data(self, email_id, result):
        """Save parsing result to database (single best match)"""
        try:
            cursor = self.db.cursor()
            
            cursor.execute('''
                INSERT INTO parsed_data 
                (email_id, category_name, parsed_fields, confidence_score, 
                 parsing_status, template_id, is_processed)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                email_id,
                result['category_name'],
                json.dumps(result['parsed_fields']),
                result['confidence_score'],
                result['parsing_status'],
                result['template_id'],
                0
            ))
            
            self.db.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f'Failed to save parsed data for email {email_id}: {e}')
            self.db.rollback()
            return False
    
    def process_batch(self, batch_size=50):
        """Process a batch of unparsed emails"""
        try:
            cursor = self.db.cursor(dictionary=True)
            
            # Find emails without parsed_data entries
            cursor.execute('''
                SELECT e.id, e.subject, e.body_text, e.body_html
                FROM emails e
                LEFT JOIN parsed_data pd ON e.id = pd.email_id
                WHERE pd.email_id IS NULL
                ORDER BY e.id ASC
                LIMIT %s
            ''', (batch_size,))
            
            emails = cursor.fetchall()
            cursor.close()
            
            if not emails:
                return 0
            
            processed = 0
            for email in emails:
                email_id = email['id']
                subject = email['subject'] or ''
                body_text = email['body_text'] or ''
                body_html = email['body_html'] or ''
                
                # Try all templates and find the BEST match
                best_result = None
                best_score = 0
                
                for template in self.templates:
                    result = self.parse_with_template(template, subject, body_text, body_html)
                    if result:
                        # Score = confidence * matches (prioritize more matches and higher confidence)
                        score = result['confidence_score'] * result['matches']
                        if score > best_score:
                            best_score = score
                            best_result = result
                
                # Save only the BEST result
                if best_result:
                    if self.save_parsed_data(email_id, best_result):
                        processed += 1
                        logger.info(f'Parsed email {email_id} as {best_result["category_name"]} (confidence: {best_result["confidence_score"]:.2f}, status: {best_result["parsing_status"]})')
                else:
                    # No matches - create failed entry
                    cursor = self.db.cursor()
                    cursor.execute('''
                        INSERT INTO parsed_data 
                        (email_id, category_name, parsed_fields, confidence_score, 
                         parsing_status, is_processed)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (email_id, 'uncategorized', '{}', 0.0, 'failed', 0))
                    self.db.commit()
                    cursor.close()
                    processed += 1
                    logger.debug(f'Email {email_id} - no template matches')
            
            logger.info(f'Processed {processed}/{len(emails)} emails')
            return processed
            
        except Exception as e:
            logger.error(f'Error processing batch: {e}')
            return 0
    
    def run_continuous(self, interval=10):
        """Run continuously"""
        logger.info('Email Parsing Service started (continuous mode)')
        
        if not self.connect_db():
            logger.error('Failed to connect to database')
            return
        
        if not self.load_templates():
            logger.error('Failed to load templates')
            return
        
        while True:
            try:
                count = self.process_batch(batch_size=50)
                if count == 0:
                    time.sleep(interval)
                else:
                    time.sleep(1)  # Process next batch quickly
            except KeyboardInterrupt:
                logger.info('Service stopped by user')
                break
            except Exception as e:
                logger.error(f'Error in main loop: {e}')
                time.sleep(interval)
    
    def run_once(self):
        """Run once to process all backlog"""
        logger.info('Email Parsing Service started (one-time mode)')
        
        if not self.connect_db():
            logger.error('Failed to connect to database')
            return
        
        if not self.load_templates():
            logger.error('Failed to load templates')
            return
        
        total = 0
        while True:
            count = self.process_batch(batch_size=100)
            total += count
            if count == 0:
                break
        
        logger.info(f'One-time processing complete: {total} emails processed')

if __name__ == '__main__':
    service = EmailParsingService()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        service.run_once()
    else:
        service.run_continuous(interval=10)
