#!/usr/bin/python3
"""
Aggressive Turo Duplicate Detection Engine
Specifically designed to catch Turo's exact duplication patterns where emails are sent twice
with identical content but different subject encoding (plain text vs UTF-8)
"""

import re
import hashlib
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'SecureRootPass123!',
    'database': 'email_server',
    'charset': 'utf8mb4'
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/turo_aggressive_duplicates.log'),
        logging.StreamHandler()
    ]
)

class AggressiveTuroDuplicateDetector:
    def __init__(self):
        self.db_connection = None
        
    def connect_database(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def close_database(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
            logging.info("Database connection closed")
    
    def normalize_subject_for_comparison(self, subject: str) -> str:
        """Normalize subject for comparison by removing UTF-8 encoding differences"""
        if not subject:
            return ""
        
        # Decode UTF-8 encoded subjects
        import email.header
        try:
            decoded_parts = email.header.decode_header(subject)
            decoded_subject = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    if encoding:
                        decoded_subject += part.decode(encoding)
                    else:
                        decoded_subject += part.decode('utf-8', errors='ignore')
                else:
                    decoded_subject += part
            subject = decoded_subject
        except:
            pass  # If decoding fails, use original
        
        # Remove company prefixes that vary
        subject = re.sub(r'^\(.*?Inc\.?.*?\)\s*-\s*', '', subject)
        subject = re.sub(r'^\(.*?LLC\.?.*?\)\s*-\s*', '', subject)
        subject = re.sub(r'^\(.*?Corp\.?.*?\)\s*-\s*', '', subject)
        
        # Normalize whitespace and case
        subject = re.sub(r'\s+', ' ', subject.lower().strip())
        
        return subject
    
    def normalize_body_content(self, body: str) -> str:
        """Normalize body content for exact comparison"""
        if not body:
            return ""
        
        # Remove all whitespace variations
        body = re.sub(r'\s+', ' ', body.strip())
        
        # Remove common email formatting
        body = re.sub(r'\n+', '\n', body)
        
        # Convert to lowercase for comparison
        body = body.lower()
        
        return body
    
    def extract_core_message_content(self, body: str) -> str:
        """Extract the core message content from Turo emails"""
        if not body:
            return ""
        
        # Pattern for guest messages: "Guest has sent you a message about your Vehicle.\n\n        [MESSAGE]"
        message_pattern = r'has sent you a message about your.*?\.\s*\n\s*\n\s*(.*?)\s*\n\s*Reply'
        match = re.search(message_pattern, body, re.DOTALL | re.IGNORECASE)
        
        if match:
            return match.group(1).strip().lower()
        
        # For other types, extract meaningful content after normalization
        normalized = self.normalize_body_content(body)
        
        # Take the middle portion which usually contains the actual content
        lines = normalized.split('\n')
        meaningful_lines = [line for line in lines if len(line.strip()) > 10]
        
        if meaningful_lines:
            # Take the core content (skip headers/footers)
            start_idx = min(2, len(meaningful_lines) // 4)
            end_idx = max(len(meaningful_lines) - 2, len(meaningful_lines) * 3 // 4)
            core_content = ' '.join(meaningful_lines[start_idx:end_idx])
            return core_content[:500]  # First 500 chars of core content
        
        return normalized[:500]
    
    def are_emails_duplicates(self, email1: Dict, email2: Dict) -> Tuple[bool, float, str]:
        """Determine if two emails are duplicates using aggressive matching"""
        
        # Method 1: Exact body content match (most reliable for Turo)
        body1_normalized = self.normalize_body_content(email1.get('body_text', ''))
        body2_normalized = self.normalize_body_content(email2.get('body_text', ''))
        
        if body1_normalized and body2_normalized:
            if body1_normalized == body2_normalized:
                return True, 1.00, "exact_body_content"
        
        # Method 2: Core message content extraction
        core1 = self.extract_core_message_content(email1.get('body_text', ''))
        core2 = self.extract_core_message_content(email2.get('body_text', ''))
        
        if core1 and core2 and len(core1) > 20 and len(core2) > 20:
            if core1 == core2:
                return True, 0.99, "exact_message_content"
        
        # Method 3: Subject normalization + body similarity
        subject1_norm = self.normalize_subject_for_comparison(email1.get('subject', ''))
        subject2_norm = self.normalize_subject_for_comparison(email2.get('subject', ''))
        
        if subject1_norm and subject2_norm and subject1_norm == subject2_norm:
            # Same normalized subject, check body similarity
            if body1_normalized and body2_normalized:
                # Calculate simple similarity
                shorter = min(len(body1_normalized), len(body2_normalized))
                longer = max(len(body1_normalized), len(body2_normalized))
                
                if shorter > 0:
                    # Check if shorter is contained in longer (allowing for minor differences)
                    similarity = shorter / longer
                    if similarity > 0.85:
                        return True, similarity, "normalized_subject_similar_body"
        
        # Method 4: Very similar body content (for slight variations)
        if body1_normalized and body2_normalized and len(body1_normalized) > 100 and len(body2_normalized) > 100:
            # Check character-level similarity
            from difflib import SequenceMatcher
            similarity = SequenceMatcher(None, body1_normalized, body2_normalized).ratio()
            
            if similarity > 0.95:
                return True, similarity, "high_body_similarity"
        
        return False, 0.0, "no_match"
    
    def find_duplicates_for_email(self, email_id: int) -> List[Dict]:
        """Find all duplicates for a specific email using aggressive matching"""
        duplicates = []
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get the target email
            cursor.execute("SELECT * FROM emails WHERE id = %s", (email_id,))
            target_email = cursor.fetchone()
            
            if not target_email:
                return duplicates
            
            # Look for potential duplicates within a very short time window (5 minutes)
            # and from the same sender
            query = """
                SELECT id, sender_email, subject, body_text, received_date
                FROM emails 
                WHERE id != %s 
                AND sender_email = %s
                AND ABS(TIMESTAMPDIFF(SECOND, received_date, %s)) <= 300
                AND is_duplicate = FALSE
                ORDER BY ABS(TIMESTAMPDIFF(SECOND, received_date, %s))
                LIMIT 20
            """
            
            cursor.execute(query, (
                email_id, 
                target_email['sender_email'],
                target_email['received_date'],
                target_email['received_date']
            ))
            
            potential_duplicates = cursor.fetchall()
            
            logging.info(f"Checking {len(potential_duplicates)} potential duplicates for email {email_id}")
            
            for candidate in potential_duplicates:
                is_duplicate, similarity, method = self.are_emails_duplicates(target_email, candidate)
                
                if is_duplicate:
                    duplicates.append({
                        'email_id': candidate['id'],
                        'similarity_score': similarity,
                        'duplicate_type': 'turo_exact_duplicate',
                        'detection_method': method,
                        'time_diff_seconds': abs((target_email['received_date'] - candidate['received_date']).total_seconds())
                    })
                    
                    logging.info(f"Found duplicate: {email_id} -> {candidate['id']} "
                               f"(similarity: {similarity:.3f}, method: {method})")
            
            return duplicates
            
        except Exception as e:
            logging.error(f"Error finding duplicates for email {email_id}: {e}")
            return []
        finally:
            cursor.close()
    
    def mark_as_duplicate(self, primary_email_id: int, duplicate_email_id: int, 
                         similarity_score: float, duplicate_type: str, detection_method: str):
        """Mark an email as duplicate of another"""
        try:
            cursor = self.db_connection.cursor()
            
            # Insert duplicate record
            query = """
                INSERT INTO email_duplicates 
                (primary_email_id, duplicate_email_id, similarity_score, duplicate_type, detection_method)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                similarity_score = VALUES(similarity_score),
                duplicate_type = VALUES(duplicate_type),
                detection_method = VALUES(detection_method)
            """
            
            cursor.execute(query, (
                primary_email_id, duplicate_email_id, similarity_score, 
                duplicate_type, detection_method
            ))
            
            # Update email status
            cursor.execute("""
                UPDATE emails 
                SET is_duplicate = TRUE, duplicate_of_email_id = %s 
                WHERE id = %s
            """, (primary_email_id, duplicate_email_id))
            
            self.db_connection.commit()
            logging.info(f"Marked email {duplicate_email_id} as duplicate of {primary_email_id} "
                        f"(method: {detection_method}, similarity: {similarity_score:.3f})")
            
        except Exception as e:
            logging.error(f"Failed to mark duplicate: {e}")
        finally:
            cursor.close()
    
    def process_all_turo_emails(self) -> Dict:
        """Process all Turo emails for aggressive duplicate detection"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get all non-duplicate Turo emails
            query = """
                SELECT id 
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
                AND is_duplicate = FALSE
                ORDER BY received_date, id
            """
            
            cursor.execute(query)
            emails = cursor.fetchall()
            
            results = {
                'processed': 0,
                'duplicates_found': 0,
                'total_emails': len(emails)
            }
            
            logging.info(f"Processing {len(emails)} Turo emails for aggressive duplicate detection")
            
            for email in emails:
                email_id = email['id']
                
                # Skip if already marked as duplicate
                cursor.execute("SELECT is_duplicate FROM emails WHERE id = %s", (email_id,))
                current_status = cursor.fetchone()
                if current_status and current_status['is_duplicate']:
                    continue
                
                duplicates = self.find_duplicates_for_email(email_id)
                
                if duplicates:
                    # Mark the best duplicate (highest similarity)
                    best_duplicate = max(duplicates, key=lambda x: x['similarity_score'])
                    
                    # Determine which should be primary (keep the earlier one)
                    cursor.execute("SELECT received_date FROM emails WHERE id = %s", (email_id,))
                    email_date = cursor.fetchone()['received_date']
                    
                    cursor.execute("SELECT received_date FROM emails WHERE id = %s", (best_duplicate['email_id'],))
                    duplicate_date = cursor.fetchone()['received_date']
                    
                    if email_date <= duplicate_date:
                        # Current email is earlier, mark the other as duplicate
                        self.mark_as_duplicate(
                            email_id, 
                            best_duplicate['email_id'],
                            best_duplicate['similarity_score'],
                            best_duplicate['duplicate_type'],
                            best_duplicate['detection_method']
                        )
                    else:
                        # Other email is earlier, mark current as duplicate
                        self.mark_as_duplicate(
                            best_duplicate['email_id'],
                            email_id,
                            best_duplicate['similarity_score'],
                            best_duplicate['duplicate_type'],
                            best_duplicate['detection_method']
                        )
                    
                    results['duplicates_found'] += 1
                
                results['processed'] += 1
                
                # Log progress every 50 emails
                if results['processed'] % 50 == 0:
                    logging.info(f"Processed {results['processed']}/{len(emails)} emails, "
                               f"found {results['duplicates_found']} duplicates")
            
            logging.info(f"Aggressive duplicate detection complete: {results}")
            return results
            
        except Exception as e:
            logging.error(f"Error in aggressive duplicate detection: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
    
    def analyze_consecutive_pairs(self) -> Dict:
        """Analyze consecutive email pairs to understand duplication patterns"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get consecutive pairs of Turo emails
            query = """
                SELECT 
                    e1.id as id1,
                    e2.id as id2,
                    e1.subject as subject1,
                    e2.subject as subject2,
                    e1.body_text as body1,
                    e2.body_text as body2,
                    e1.received_date as date1,
                    e2.received_date as date2,
                    TIMESTAMPDIFF(SECOND, e1.received_date, e2.received_date) as seconds_apart
                FROM emails e1
                JOIN emails e2 ON e2.id = e1.id + 1
                WHERE e1.sender_email = 'noreply@mail.turo.com'
                AND e2.sender_email = 'noreply@mail.turo.com'
                AND e1.is_duplicate = FALSE
                AND e2.is_duplicate = FALSE
                ORDER BY e1.id
                LIMIT 100
            """
            
            cursor.execute(query)
            pairs = cursor.fetchall()
            
            analysis = {
                'total_pairs': len(pairs),
                'exact_duplicates': 0,
                'subject_encoding_differences': 0,
                'time_differences': [],
                'duplicate_pairs': []
            }
            
            for pair in pairs:
                is_duplicate, similarity, method = self.are_emails_duplicates(
                    {'subject': pair['subject1'], 'body_text': pair['body1']},
                    {'subject': pair['subject2'], 'body_text': pair['body2']}
                )
                
                if is_duplicate:
                    analysis['exact_duplicates'] += 1
                    analysis['duplicate_pairs'].append({
                        'id1': pair['id1'],
                        'id2': pair['id2'],
                        'similarity': similarity,
                        'method': method,
                        'seconds_apart': pair['seconds_apart']
                    })
                    
                    # Check if subjects are different (encoding issue)
                    if pair['subject1'] != pair['subject2']:
                        analysis['subject_encoding_differences'] += 1
                
                analysis['time_differences'].append(pair['seconds_apart'])
            
            # Calculate statistics
            if analysis['time_differences']:
                analysis['avg_time_diff'] = sum(analysis['time_differences']) / len(analysis['time_differences'])
                analysis['max_time_diff'] = max(analysis['time_differences'])
                analysis['min_time_diff'] = min(analysis['time_differences'])
            
            logging.info(f"Consecutive pair analysis: {analysis['exact_duplicates']}/{analysis['total_pairs']} are duplicates")
            return analysis
            
        except Exception as e:
            logging.error(f"Error in consecutive pair analysis: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Aggressive Turo Duplicate Detection")
    parser.add_argument('--analyze', action='store_true', help='Analyze consecutive pairs')
    parser.add_argument('--process-all', action='store_true', help='Process all emails for duplicates')
    parser.add_argument('--email-id', type=int, help='Find duplicates for specific email')
    
    args = parser.parse_args()
    
    detector = AggressiveTuroDuplicateDetector()
    detector.connect_database()
    
    try:
        if args.analyze:
            result = detector.analyze_consecutive_pairs()
            print(f"Analysis result: {result}")
        elif args.process_all:
            result = detector.process_all_turo_emails()
            print(f"Processing result: {result}")
        elif args.email_id:
            duplicates = detector.find_duplicates_for_email(args.email_id)
            print(f"Duplicates for email {args.email_id}: {duplicates}")
        else:
            print("Please specify --analyze, --process-all, or --email-id")
    
    finally:
        detector.close_database()

if __name__ == "__main__":
    main()
