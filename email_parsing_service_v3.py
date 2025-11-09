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
    'database': 'email_server',
    'autocommit': True
}

LOG_FILE = '/root/email-system/logs/parsing_service.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE)]
)

logger = logging.getLogger(__name__)

class EmailParsingService:
    def __init__(self):
        self.templates = []
        
    def get_db_connection(self):
        """Create a fresh database connection"""
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            logger.error(f'DB connection failed: {e}')
            return None
    
    def load_templates(self):
        """Load parsing templates from database"""
        try:
            conn = self.get_db_connection()
            if not conn:
                return False
                
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT id, category_name, template_name, field_extractions
                FROM parsing_rule_templates
                WHERE is_active = 1
                ORDER BY id ASC
            ''')
            self.templates = cursor.fetchall()
            cursor.close()
            conn.close()
            logger.info(f'Loaded {len(self.templates)} templates')
            return True
        except Exception as e:
            logger.error(f'Failed to load templates: {e}')
            return False
    
    def parse_with_template(self, template, subject, body_text, body_html):
        """Parse email using a template"""
        try:
            field_extractions = json.loads(template['field_extractions'])
            
            if not isinstance(field_extractions, dict):
                return None
            
            parsed_fields = {}
            matches = 0
            total_fields = len(field_extractions)
            
            for field_name, field_config in field_extractions.items():
                try:
                    if isinstance(field_config, dict):
                        pattern = field_config.get('pattern', '')
                        source = field_config.get('source', 'body')
                        group = field_config.get('group', 1)
                    elif isinstance(field_config, list):
                        pattern = field_config[0] if field_config else ''
                        source = 'body'
                        group = 1
                    else:
                        continue
                    
                    if source == 'subject':
                        text = subject or ''
                    elif source == 'html':
                        text = body_html or ''
                    else:
                        text = body_text or ''
                    
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
                    continue
            
            if matches == 0:
                return None
            
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
            return None
    
    def process_batch(self, batch_size=50):
        """Process a batch of unparsed emails with fresh connection"""
        conn = None
        try:
            conn = self.get_db_connection()
            if not conn:
                return 0
                
            cursor = conn.cursor(dictionary=True)
            
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
                conn.close()
                return 0
            
            processed = 0
            for email in emails:
                email_id = email['id']
                subject = email['subject'] or ''
                body_text = email['body_text'] or ''
                body_html = email['body_html'] or ''
                
                best_result = None
                best_score = 0
                
                for template in self.templates:
                    result = self.parse_with_template(template, subject, body_text, body_html)
                    if result:
                        score = result['confidence_score'] * result['matches']
                        if score > best_score:
                            best_score = score
                            best_result = result
                
                cursor = conn.cursor()
                if best_result:
                    cursor.execute('''
                        INSERT INTO parsed_data 
                        (email_id, category_name, parsed_fields, confidence_score, 
                         parsing_status, template_id, is_processed)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        email_id,
                        best_result['category_name'],
                        json.dumps(best_result['parsed_fields']),
                        best_result['confidence_score'],
                        best_result['parsing_status'],
                        best_result['template_id'],
                        0
                    ))
                    logger.info(f'Parsed email {email_id} as {best_result["category_name"]}')
                else:
                    cursor.execute('''
                        INSERT INTO parsed_data 
                        (email_id, category_name, parsed_fields, confidence_score, 
                         parsing_status, is_processed)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (email_id, 'uncategorized', '{}', 0.0, 'failed', 0))
                
                cursor.close()
                processed += 1
            
            conn.close()
            logger.info(f'Processed {processed}/{len(emails)} emails')
            return processed
            
        except Exception as e:
            logger.error(f'Error processing batch: {e}')
            if conn:
                conn.close()
            return 0
    
    def run_continuous(self, interval=10):
        """Run continuously with fresh connections each iteration"""
        logger.info('Email Parsing Service v3 started (continuous mode)')
        
        if not self.load_templates():
            logger.error('Failed to load templates')
            return
        
        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info(f'Iteration {iteration}: Checking for new emails...')
                
                count = self.process_batch(batch_size=50)
                
                if count == 0:
                    logger.info(f'No emails to process, sleeping {interval}s')
                    time.sleep(interval)
                else:
                    logger.info(f'Processed {count} emails, checking for more...')
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info('Service stopped by user')
                break
            except Exception as e:
                logger.error(f'Error in main loop: {e}')
                time.sleep(interval)

if __name__ == '__main__':
    service = EmailParsingService()
    service.run_continuous(interval=10)
