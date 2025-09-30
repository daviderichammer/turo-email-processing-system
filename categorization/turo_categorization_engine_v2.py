#!/usr/bin/python3
"""
Turo Email Dynamic Categorization and Duplicate Detection Engine - Version 2
Enhanced duplicate detection that ignores subject lines for Turo's multi-recipient pattern
"""

import re
import json
import hashlib
import mysql.connector
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from difflib import SequenceMatcher
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
        logging.FileHandler('/var/log/turo_categorization.log'),
        logging.StreamHandler()
    ]
)

class TuroCategorizer:
    def __init__(self):
        self.db_connection = None
        self.categories = {}
        self.patterns = {}
        
    def connect_database(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            logging.info("Database connection established")
            self.load_categories_and_patterns()
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def close_database(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
            logging.info("Database connection closed")
    
    def load_categories_and_patterns(self):
        """Load categories and patterns from database"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Load categories
            cursor.execute("SELECT * FROM email_categories WHERE auto_assign = TRUE")
            categories = cursor.fetchall()
            
            for category in categories:
                self.categories[category['id']] = category
            
            # Load patterns
            cursor.execute("""
                SELECT cp.*, ec.category_name 
                FROM category_patterns cp
                JOIN email_categories ec ON cp.category_id = ec.id
                WHERE cp.is_active = TRUE
                ORDER BY cp.success_rate DESC, cp.pattern_weight DESC
            """)
            patterns = cursor.fetchall()
            
            for pattern in patterns:
                category_id = pattern['category_id']
                if category_id not in self.patterns:
                    self.patterns[category_id] = []
                self.patterns[category_id].append(pattern)
            
            logging.info(f"Loaded {len(self.categories)} categories and {len(patterns)} patterns")
            
        except Exception as e:
            logging.error(f"Failed to load categories and patterns: {e}")
            raise
        finally:
            cursor.close()
    
    def normalize_email_content(self, email_data: Dict) -> str:
        """Normalize email content for duplicate detection, ignoring subject variations"""
        content_parts = []
        
        # Always include sender (this should be consistent for Turo emails)
        if email_data.get('sender_email'):
            content_parts.append(email_data['sender_email'].lower().strip())
        
        # Process body content - this is the key for detecting Turo duplicates
        if email_data.get('body_text'):
            body = email_data['body_text']
            
            # Remove dynamic elements that change between recipients
            # Remove URLs with reservation IDs or user-specific tokens
            body = re.sub(r'https://turo\.com/[^\s]+', '[TURO_URL]', body)
            
            # Remove email addresses (recipient-specific)
            body = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', body)
            
            # Remove phone numbers
            body = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE]', body)
            
            # Remove specific dollar amounts (these might vary)
            body = re.sub(r'\$\d+(?:\.\d{2})?', '[AMOUNT]', body)
            
            # Remove dates and times that might be recipient-specific
            body = re.sub(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b', '[DATE]', body)
            body = re.sub(r'\b\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?\b', '[TIME]', body)
            
            # Remove extra whitespace and normalize
            body = re.sub(r'\s+', ' ', body.lower().strip())
            
            # Take first 1000 characters for efficiency while maintaining uniqueness
            content_parts.append(body[:1000])
        
        # Create content string (NOTE: Subject is intentionally excluded)
        content_string = '|'.join(content_parts)
        return content_string
    
    def generate_content_hash(self, email_data: Dict) -> str:
        """Generate a hash for email content to detect exact duplicates (ignoring subject)"""
        normalized_content = self.normalize_email_content(email_data)
        return hashlib.md5(normalized_content.encode('utf-8')).hexdigest()
    
    def calculate_content_similarity(self, email1_data: Dict, email2_data: Dict) -> float:
        """Calculate similarity between email contents (ignoring subjects)"""
        content1 = self.normalize_email_content(email1_data)
        content2 = self.normalize_email_content(email2_data)
        
        if not content1 or not content2:
            return 0.0
        
        # Calculate similarity using SequenceMatcher
        similarity = SequenceMatcher(None, content1, content2).ratio()
        return similarity
    
    def extract_turo_message_signature(self, email_data: Dict) -> Optional[str]:
        """Extract a signature from Turo messages that should be identical across recipients"""
        body = email_data.get('body_text', '')
        
        # Look for the actual message content in Turo emails
        # Pattern: "Guest has sent you a message about your Vehicle.\n\n        [MESSAGE CONTENT]"
        message_pattern = r'has sent you a message about your.*?\.\s*\n\s*\n\s*(.*?)\s*\n\s*Reply'
        match = re.search(message_pattern, body, re.DOTALL | re.IGNORECASE)
        
        if match:
            message_content = match.group(1).strip()
            # Normalize the message content
            message_content = re.sub(r'\s+', ' ', message_content.lower())
            return message_content
        
        # For other types of Turo emails, look for consistent content patterns
        # Remove recipient-specific information and extract core message
        normalized_body = body.lower()
        
        # Remove common variable elements
        normalized_body = re.sub(r'https://[^\s]+', '', normalized_body)
        normalized_body = re.sub(r'\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b', '', normalized_body)
        normalized_body = re.sub(r'\$\d+(?:\.\d{2})?', '', normalized_body)
        normalized_body = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '', normalized_body)
        
        # Extract meaningful content (first 200 chars after normalization)
        normalized_body = re.sub(r'\s+', ' ', normalized_body.strip())
        return normalized_body[:200] if normalized_body else None
    
    def detect_duplicates(self, email_id: int, email_data: Dict) -> List[Dict]:
        """Detect duplicate emails using content-based matching (ignoring subjects)"""
        duplicates = []
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Method 1: Exact content hash matching (ignoring subject)
            content_hash = self.generate_content_hash(email_data)
            
            # Method 2: Turo message signature matching
            message_signature = self.extract_turo_message_signature(email_data)
            
            # Check for emails with similar content from the same sender within a reasonable timeframe
            query = """
                SELECT id, sender_email, subject, body_text, received_date
                FROM emails 
                WHERE id != %s 
                AND sender_email = %s
                AND received_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                AND is_duplicate = FALSE
                ORDER BY received_date DESC
                LIMIT 100
            """
            
            cursor.execute(query, (email_id, email_data.get('sender_email', '')))
            similar_emails = cursor.fetchall()
            
            logging.info(f"Checking {len(similar_emails)} potential duplicates for email {email_id}")
            
            for similar_email in similar_emails:
                # Method 1: Exact content hash match
                similar_hash = self.generate_content_hash(similar_email)
                if content_hash == similar_hash:
                    duplicates.append({
                        'email_id': similar_email['id'],
                        'similarity_score': 1.00,
                        'duplicate_type': 'exact_content',
                        'detection_method': 'content_hash_no_subject'
                    })
                    logging.info(f"Found exact content duplicate: {email_id} -> {similar_email['id']}")
                    continue
                
                # Method 2: Turo message signature matching
                if message_signature:
                    similar_signature = self.extract_turo_message_signature(similar_email)
                    if similar_signature and message_signature == similar_signature:
                        duplicates.append({
                            'email_id': similar_email['id'],
                            'similarity_score': 0.98,
                            'duplicate_type': 'same_message',
                            'detection_method': 'turo_message_signature'
                        })
                        logging.info(f"Found message signature duplicate: {email_id} -> {similar_email['id']}")
                        continue
                
                # Method 3: Content similarity (ignoring subject)
                content_similarity = self.calculate_content_similarity(email_data, similar_email)
                
                if content_similarity >= 0.95:
                    duplicates.append({
                        'email_id': similar_email['id'],
                        'similarity_score': content_similarity,
                        'duplicate_type': 'near_exact_content',
                        'detection_method': 'content_similarity_no_subject'
                    })
                    logging.info(f"Found high content similarity duplicate: {email_id} -> {similar_email['id']} (score: {content_similarity:.3f})")
                elif content_similarity >= 0.85:
                    duplicates.append({
                        'email_id': similar_email['id'],
                        'similarity_score': content_similarity,
                        'duplicate_type': 'similar_content',
                        'detection_method': 'content_similarity_no_subject'
                    })
                    logging.info(f"Found content similarity duplicate: {email_id} -> {similar_email['id']} (score: {content_similarity:.3f})")
            
            logging.info(f"Found {len(duplicates)} potential duplicates for email {email_id}")
            return duplicates
            
        except Exception as e:
            logging.error(f"Duplicate detection failed for email {email_id}: {e}")
            return []
        finally:
            cursor.close()
    
    def categorize_email(self, email_id: int, email_data: Dict) -> List[Dict]:
        """Categorize email using pattern matching"""
        category_scores = {}
        
        try:
            # Test each category's patterns
            for category_id, patterns in self.patterns.items():
                total_score = 0.0
                total_weight = 0.0
                
                for pattern in patterns:
                    pattern_regex = pattern['pattern_regex']
                    pattern_weight = float(pattern['pattern_weight'])
                    pattern_type = pattern['pattern_type']
                    
                    # Get the text to match against
                    text_to_match = ""
                    if pattern_type == 'subject':
                        text_to_match = email_data.get('subject', '')
                    elif pattern_type == 'sender':
                        text_to_match = email_data.get('sender_email', '')
                    elif pattern_type == 'body':
                        text_to_match = email_data.get('body_text', '')
                    elif pattern_type == 'combined':
                        text_to_match = f"{email_data.get('subject', '')} {email_data.get('body_text', '')}"
                    
                    # Test pattern match
                    try:
                        if re.search(pattern_regex, text_to_match, re.IGNORECASE | re.DOTALL):
                            total_score += pattern_weight
                        total_weight += pattern_weight
                    except re.error as e:
                        logging.warning(f"Invalid regex pattern {pattern_regex}: {e}")
                        continue
                
                # Calculate normalized score
                if total_weight > 0:
                    normalized_score = total_score / total_weight
                    category_scores[category_id] = normalized_score
            
            # Convert to list of results
            results = []
            for category_id, score in category_scores.items():
                category = self.categories[category_id]
                if score >= category['confidence_threshold']:
                    results.append({
                        'category_id': category_id,
                        'category_name': category['category_name'],
                        'confidence_score': score,
                        'assignment_method': 'auto'
                    })
            
            # Sort by confidence score
            results.sort(key=lambda x: x['confidence_score'], reverse=True)
            
            logging.info(f"Email {email_id} categorization results: {len(results)} matches")
            return results
            
        except Exception as e:
            logging.error(f"Categorization failed for email {email_id}: {e}")
            return []
    
    def suggest_new_category(self, email_ids: List[int], pattern_analysis: Dict) -> Optional[int]:
        """Suggest a new category based on uncategorized emails"""
        try:
            cursor = self.db_connection.cursor()
            
            # Generate category name based on patterns
            suggested_name = self.generate_category_name(pattern_analysis)
            
            # Create suggestion
            query = """
                INSERT INTO category_suggestions 
                (suggested_name, description, sample_email_ids, pattern_analysis, suggestion_confidence)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            description = f"Auto-suggested category based on {len(email_ids)} similar emails"
            
            cursor.execute(query, (
                suggested_name,
                description,
                json.dumps(email_ids),
                json.dumps(pattern_analysis),
                pattern_analysis.get('confidence', 0.70)
            ))
            
            suggestion_id = cursor.lastrowid
            self.db_connection.commit()
            
            logging.info(f"Created category suggestion: {suggested_name} (ID: {suggestion_id})")
            return suggestion_id
            
        except Exception as e:
            logging.error(f"Failed to create category suggestion: {e}")
            return None
        finally:
            cursor.close()
    
    def generate_category_name(self, pattern_analysis: Dict) -> str:
        """Generate a category name based on pattern analysis"""
        # Extract key terms from subject patterns
        subject_patterns = pattern_analysis.get('subject_patterns', [])
        
        if subject_patterns:
            # Look for common terms
            common_terms = []
            for pattern in subject_patterns:
                # Extract meaningful words (not common words)
                words = re.findall(r'\b[A-Za-z]{3,}\b', pattern.lower())
                for word in words:
                    if word not in ['has', 'sent', 'you', 'message', 'about', 'your', 'the', 'and', 'for']:
                        common_terms.append(word)
            
            if common_terms:
                # Use most common term
                from collections import Counter
                most_common = Counter(common_terms).most_common(1)[0][0]
                return f"{most_common}_notification"
        
        # Fallback to generic name
        return f"unknown_category_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
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
            logging.info(f"Marked email {duplicate_email_id} as duplicate of {primary_email_id} (method: {detection_method})")
            
        except Exception as e:
            logging.error(f"Failed to mark duplicate: {e}")
        finally:
            cursor.close()
    
    def assign_category(self, email_id: int, category_id: int, confidence_score: float, 
                       assignment_method: str = 'auto'):
        """Assign a category to an email"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO email_category_assignments 
                (email_id, category_id, confidence_score, assignment_method)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                confidence_score = VALUES(confidence_score),
                assignment_method = VALUES(assignment_method),
                assigned_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(query, (email_id, category_id, confidence_score, assignment_method))
            self.db_connection.commit()
            
            logging.info(f"Assigned email {email_id} to category {category_id} with confidence {confidence_score}")
            
        except Exception as e:
            logging.error(f"Failed to assign category: {e}")
        finally:
            cursor.close()
    
    def update_processing_status(self, email_id: int, **status_updates):
        """Update processing status for an email"""
        try:
            cursor = self.db_connection.cursor()
            
            # Build dynamic update query
            set_clauses = []
            values = []
            
            for field, value in status_updates.items():
                set_clauses.append(f"{field} = %s")
                values.append(value)
            
            if set_clauses:
                query = f"""
                    INSERT INTO email_processing_status (email_id, {', '.join(status_updates.keys())})
                    VALUES (%s, {', '.join(['%s'] * len(values))})
                    ON DUPLICATE KEY UPDATE
                    {', '.join(set_clauses)},
                    last_processed_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(query, [email_id] + values + values)
                self.db_connection.commit()
            
        except Exception as e:
            logging.error(f"Failed to update processing status: {e}")
        finally:
            cursor.close()
    
    def process_email(self, email_id: int) -> Dict:
        """Process a single email for categorization and duplicate detection"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get email data
            cursor.execute("SELECT * FROM emails WHERE id = %s", (email_id,))
            email_data = cursor.fetchone()
            
            if not email_data:
                logging.error(f"Email {email_id} not found")
                return {'success': False, 'error': 'Email not found'}
            
            results = {
                'email_id': email_id,
                'success': True,
                'categories': [],
                'duplicates': [],
                'suggestions': []
            }
            
            # Step 1: Enhanced Duplicate Detection (ignoring subject)
            logging.info(f"Processing email {email_id}: enhanced duplicate detection (ignoring subject)")
            duplicates = self.detect_duplicates(email_id, email_data)
            
            if duplicates:
                # Mark as duplicate if high similarity found
                best_duplicate = max(duplicates, key=lambda x: x['similarity_score'])
                if best_duplicate['similarity_score'] >= 0.85:  # Lowered threshold since we're more precise
                    self.mark_as_duplicate(
                        best_duplicate['email_id'], email_id,
                        best_duplicate['similarity_score'],
                        best_duplicate['duplicate_type'],
                        best_duplicate['detection_method']
                    )
                    self.update_processing_status(email_id, duplicate_check_status='is_duplicate')
                    results['duplicates'] = duplicates
                    return results
            
            self.update_processing_status(email_id, duplicate_check_status='checked')
            
            # Step 2: Categorization
            logging.info(f"Processing email {email_id}: categorization")
            categories = self.categorize_email(email_id, email_data)
            
            if categories:
                # Assign best matching category
                best_category = categories[0]
                self.assign_category(
                    email_id, 
                    best_category['category_id'],
                    best_category['confidence_score'],
                    best_category['assignment_method']
                )
                self.update_processing_status(email_id, categorization_status='categorized')
                results['categories'] = categories
            else:
                # No category match - suggest new category
                logging.info(f"No category match for email {email_id}, analyzing for new category")
                self.update_processing_status(email_id, categorization_status='suggested')
                
                # Analyze pattern for suggestion (simplified)
                pattern_analysis = {
                    'subject_patterns': [email_data.get('subject', '')],
                    'sender_patterns': [email_data.get('sender_email', '')],
                    'confidence': 0.70
                }
                
                suggestion_id = self.suggest_new_category([email_id], pattern_analysis)
                if suggestion_id:
                    results['suggestions'].append({
                        'suggestion_id': suggestion_id,
                        'pattern_analysis': pattern_analysis
                    })
            
            return results
            
        except Exception as e:
            logging.error(f"Failed to process email {email_id}: {e}")
            self.update_processing_status(email_id, 
                categorization_status='failed',
                duplicate_check_status='failed',
                processing_notes=str(e)
            )
            return {'success': False, 'error': str(e)}
        finally:
            cursor.close()
    
    def process_batch(self, limit: int = 100) -> Dict:
        """Process a batch of unprocessed emails"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get unprocessed emails
            query = """
                SELECT e.id 
                FROM emails e
                LEFT JOIN email_processing_status eps ON e.id = eps.email_id
                WHERE (eps.categorization_status IS NULL OR eps.categorization_status = 'pending')
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
                ORDER BY e.id DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            emails = cursor.fetchall()
            
            results = {
                'processed': 0,
                'categorized': 0,
                'duplicates_found': 0,
                'suggestions_created': 0,
                'errors': 0
            }
            
            for email in emails:
                email_id = email['id']
                result = self.process_email(email_id)
                
                results['processed'] += 1
                
                if result['success']:
                    if result['categories']:
                        results['categorized'] += 1
                    if result['duplicates']:
                        results['duplicates_found'] += 1
                    if result['suggestions']:
                        results['suggestions_created'] += 1
                else:
                    results['errors'] += 1
                
                # Log progress
                if results['processed'] % 10 == 0:
                    logging.info(f"Processed {results['processed']}/{len(emails)} emails")
            
            logging.info(f"Batch processing complete: {results}")
            return results
            
        except Exception as e:
            logging.error(f"Batch processing failed: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Turo Email Categorization Engine v2 - Enhanced Duplicate Detection")
    parser.add_argument('--email-id', type=int, help='Process specific email ID')
    parser.add_argument('--batch', type=int, default=100, help='Process batch of emails')
    parser.add_argument('--setup', action='store_true', help='Setup database schema')
    
    args = parser.parse_args()
    
    categorizer = TuroCategorizer()
    categorizer.connect_database()
    
    try:
        if args.setup:
            # Setup would be handled by running the SQL schema separately
            print("Run the turo_categorization_schema.sql file to setup the database")
            return
        
        if args.email_id:
            result = categorizer.process_email(args.email_id)
            print(f"Processing result: {result}")
        else:
            result = categorizer.process_batch(args.batch)
            print(f"Batch processing result: {result}")
    
    finally:
        categorizer.close_database()

if __name__ == "__main__":
    main()
