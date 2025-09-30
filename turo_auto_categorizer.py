#!/usr/bin/python3
"""
Turo Automatic Email Categorization System
Intelligently categorizes emails based on content patterns and creates a separate duplicates category
"""

import re
import mysql.connector
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import json

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
        logging.FileHandler('/var/log/turo_auto_categorizer.log'),
        logging.StreamHandler()
    ]
)

class TuroAutoCategorizer:
    def __init__(self):
        self.db_connection = None
        self.categories = {}
        self.categorization_rules = []
        
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
    
    def setup_categorization_system(self):
        """Set up the automatic categorization system with predefined rules"""
        
        # Define the categorization rules based on email analysis
        self.categorization_rules = [
            # Duplicates category (highest priority)
            {
                'category_name': 'duplicates',
                'category_description': 'Duplicate emails that should be ignored',
                'priority': 1,
                'rules': [
                    {
                        'field': 'is_duplicate',
                        'pattern': 'TRUE',
                        'type': 'exact',
                        'confidence': 100
                    }
                ]
            },
            
            # Guest Messages
            {
                'category_name': 'guest_messages',
                'category_description': 'Messages from guests about vehicles',
                'priority': 10,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*has sent you a message about your.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*has sent you a message about your.*',
                        'type': 'regex',
                        'confidence': 90
                    }
                ]
            },
            
            # Trip Bookings
            {
                'category_name': 'trip_bookings',
                'category_description': 'New trip booking confirmations',
                'priority': 20,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*trip.*is booked.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*trip is booked.*',
                        'type': 'regex',
                        'confidence': 90
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*Cha-ching.*',
                        'type': 'regex',
                        'confidence': 85
                    }
                ]
            },
            
            # Vehicle Returns
            {
                'category_name': 'vehicle_returns',
                'category_description': 'Vehicle return notifications',
                'priority': 30,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*has returned.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*has returned.*',
                        'type': 'regex',
                        'confidence': 90
                    }
                ]
            },
            
            # Trip Ratings/Reviews
            {
                'category_name': 'ratings_reviews',
                'category_description': 'Trip rating and review notifications',
                'priority': 40,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*just rated.*trip.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*rate.*trip.*',
                        'type': 'regex',
                        'confidence': 85
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*write a review.*',
                        'type': 'regex',
                        'confidence': 80
                    }
                ]
            },
            
            # Trip Modifications
            {
                'category_name': 'trip_modifications',
                'category_description': 'Trip change requests and confirmations',
                'priority': 50,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*change request.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'subject',
                        'pattern': r'.*confirmed.*change.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*requested a change.*',
                        'type': 'regex',
                        'confidence': 90
                    }
                ]
            },
            
            # Additional Drivers
            {
                'category_name': 'additional_drivers',
                'category_description': 'Additional driver notifications',
                'priority': 60,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*added.*driver.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*added.*driver.*',
                        'type': 'regex',
                        'confidence': 90
                    }
                ]
            },
            
            # License Verification
            {
                'category_name': 'license_verification',
                'category_description': 'License verification reminders',
                'priority': 70,
                'rules': [
                    {
                        'field': 'subject',
                        'pattern': r'.*confirm.*license.*',
                        'type': 'regex',
                        'confidence': 95
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*confirm.*license.*',
                        'type': 'regex',
                        'confidence': 90
                    }
                ]
            },
            
            # Payments and Payouts
            {
                'category_name': 'payments_payouts',
                'category_description': 'Payment and payout notifications',
                'priority': 80,
                'rules': [
                    {
                        'field': 'body_text',
                        'pattern': r'.*payment.*received.*',
                        'type': 'regex',
                        'confidence': 90
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*payout.*',
                        'type': 'regex',
                        'confidence': 85
                    },
                    {
                        'field': 'body_text',
                        'pattern': r'.*earnings.*',
                        'type': 'regex',
                        'confidence': 80
                    }
                ]
            }
        ]
        
        logging.info(f"Loaded {len(self.categorization_rules)} categorization rules")
    
    def create_categories_in_database(self):
        """Create category records in the database"""
        try:
            cursor = self.db_connection.cursor()
            
            for rule_set in self.categorization_rules:
                category_name = rule_set['category_name']
                description = rule_set['category_description']
                
                # Insert or update category
                query = """
                    INSERT INTO email_categories (name, description, is_active, created_at)
                    VALUES (%s, %s, TRUE, NOW())
                    ON DUPLICATE KEY UPDATE
                    description = VALUES(description),
                    updated_at = NOW()
                """
                
                cursor.execute(query, (category_name, description))
                
                # Get category ID
                cursor.execute("SELECT id FROM email_categories WHERE name = %s", (category_name,))
                category_id = cursor.fetchone()[0]
                
                # Store category mapping
                self.categories[category_name] = category_id
                
                logging.info(f"Created/updated category: {category_name} (ID: {category_id})")
            
            self.db_connection.commit()
            
        except Exception as e:
            logging.error(f"Error creating categories: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()
    
    def evaluate_email_against_rules(self, email: Dict) -> Tuple[Optional[str], int, str]:
        """Evaluate an email against all categorization rules"""
        
        best_match = None
        best_confidence = 0
        best_method = ""
        
        # Sort rules by priority (lower number = higher priority)
        sorted_rules = sorted(self.categorization_rules, key=lambda x: x['priority'])
        
        for rule_set in sorted_rules:
            category_name = rule_set['category_name']
            rules = rule_set['rules']
            
            # Check if any rule in this set matches
            for rule in rules:
                field = rule['field']
                pattern = rule['pattern']
                rule_type = rule['type']
                confidence = rule['confidence']
                
                # Get field value from email
                if field == 'is_duplicate':
                    field_value = str(email.get('is_duplicate', False))
                else:
                    field_value = email.get(field, '') or ''
                
                # Apply the rule
                matches = False
                
                if rule_type == 'exact':
                    matches = field_value.upper() == pattern.upper()
                elif rule_type == 'regex':
                    try:
                        matches = bool(re.search(pattern, field_value, re.IGNORECASE | re.DOTALL))
                    except re.error as e:
                        logging.warning(f"Invalid regex pattern '{pattern}': {e}")
                        continue
                elif rule_type == 'contains':
                    matches = pattern.lower() in field_value.lower()
                
                if matches and confidence > best_confidence:
                    best_match = category_name
                    best_confidence = confidence
                    best_method = f"{rule_type}_{field}_{pattern[:30]}"
                    
                    # If this is a high-confidence match, we can stop here
                    if confidence >= 95:
                        break
            
            # If we found a high-confidence match, stop checking other rule sets
            if best_confidence >= 95:
                break
        
        return best_match, best_confidence, best_method
    
    def categorize_email(self, email_id: int) -> bool:
        """Categorize a single email"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get email details
            cursor.execute("""
                SELECT id, sender_email, subject, body_text, is_duplicate
                FROM emails 
                WHERE id = %s
            """, (email_id,))
            
            email = cursor.fetchone()
            if not email:
                logging.warning(f"Email {email_id} not found")
                return False
            
            # Evaluate against rules
            category_name, confidence, method = self.evaluate_email_against_rules(email)
            
            if category_name and confidence > 70:  # Minimum confidence threshold
                category_id = self.categories.get(category_name)
                
                if category_id:
                    # Update email with category
                    cursor.execute("""
                        UPDATE emails 
                        SET category_id = %s, categorization_confidence = %s, categorization_method = %s
                        WHERE id = %s
                    """, (category_id, confidence, method, email_id))
                    
                    self.db_connection.commit()
                    
                    logging.info(f"Categorized email {email_id} as '{category_name}' "
                               f"(confidence: {confidence}%, method: {method})")
                    return True
            
            # If no category matched, leave uncategorized
            logging.info(f"Email {email_id} remains uncategorized (best match: {category_name}, "
                        f"confidence: {confidence}%)")
            return False
            
        except Exception as e:
            logging.error(f"Error categorizing email {email_id}: {e}")
            return False
        finally:
            cursor.close()
    
    def categorize_all_turo_emails(self) -> Dict:
        """Categorize all Turo emails automatically"""
        try:
            cursor = self.db_connection.cursor()
            
            # Get all Turo emails that need categorization
            cursor.execute("""
                SELECT id 
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
                AND (category_id IS NULL OR category_id = 0)
                ORDER BY received_date DESC
            """)
            
            email_ids = [row[0] for row in cursor.fetchall()]
            
            results = {
                'total_emails': len(email_ids),
                'categorized': 0,
                'duplicates_found': 0,
                'categories_used': {},
                'uncategorized': 0
            }
            
            logging.info(f"Starting automatic categorization of {len(email_ids)} emails")
            
            for email_id in email_ids:
                if self.categorize_email(email_id):
                    results['categorized'] += 1
                    
                    # Get the category that was assigned
                    cursor.execute("""
                        SELECT ec.name 
                        FROM emails e
                        JOIN email_categories ec ON e.category_id = ec.id
                        WHERE e.id = %s
                    """, (email_id,))
                    
                    category_result = cursor.fetchone()
                    if category_result:
                        category_name = category_result[0]
                        results['categories_used'][category_name] = results['categories_used'].get(category_name, 0) + 1
                        
                        if category_name == 'duplicates':
                            results['duplicates_found'] += 1
                else:
                    results['uncategorized'] += 1
                
                # Log progress every 50 emails
                if (results['categorized'] + results['uncategorized']) % 50 == 0:
                    processed = results['categorized'] + results['uncategorized']
                    logging.info(f"Progress: {processed}/{len(email_ids)} emails processed")
            
            logging.info(f"Automatic categorization complete: {results}")
            return results
            
        except Exception as e:
            logging.error(f"Error in automatic categorization: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
    
    def get_categorization_summary(self) -> Dict:
        """Get a summary of the categorization results"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get category breakdown
            cursor.execute("""
                SELECT 
                    COALESCE(ec.name, 'uncategorized') as category,
                    COALESCE(ec.description, 'Emails without category') as description,
                    COUNT(*) as count,
                    ROUND(AVG(COALESCE(e.categorization_confidence, 0)), 1) as avg_confidence
                FROM emails e
                LEFT JOIN email_categories ec ON e.category_id = ec.id
                WHERE e.sender_email = 'noreply@mail.turo.com'
                GROUP BY ec.id, ec.name, ec.description
                ORDER BY count DESC
            """)
            
            categories = cursor.fetchall()
            
            # Get total counts
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_emails,
                    SUM(CASE WHEN category_id IS NOT NULL AND category_id > 0 THEN 1 ELSE 0 END) as categorized,
                    SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) as duplicates
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
            """)
            
            totals = cursor.fetchone()
            
            return {
                'totals': totals,
                'categories': categories,
                'categorization_rate': round((totals['categorized'] / totals['total_emails']) * 100, 1) if totals['total_emails'] > 0 else 0
            }
            
        except Exception as e:
            logging.error(f"Error getting categorization summary: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Turo Automatic Email Categorization")
    parser.add_argument('--setup', action='store_true', help='Set up categorization system')
    parser.add_argument('--categorize-all', action='store_true', help='Categorize all emails')
    parser.add_argument('--email-id', type=int, help='Categorize specific email')
    parser.add_argument('--summary', action='store_true', help='Show categorization summary')
    
    args = parser.parse_args()
    
    categorizer = TuroAutoCategorizer()
    categorizer.connect_database()
    
    try:
        if args.setup:
            categorizer.setup_categorization_system()
            categorizer.create_categories_in_database()
            print("✅ Categorization system set up successfully")
        
        elif args.categorize_all:
            categorizer.setup_categorization_system()
            categorizer.create_categories_in_database()
            result = categorizer.categorize_all_turo_emails()
            print(f"Categorization result: {result}")
        
        elif args.email_id:
            categorizer.setup_categorization_system()
            categorizer.create_categories_in_database()
            success = categorizer.categorize_email(args.email_id)
            print(f"Email {args.email_id} categorization: {'Success' if success else 'Failed'}")
        
        elif args.summary:
            summary = categorizer.get_categorization_summary()
            print(f"Categorization summary: {json.dumps(summary, indent=2, default=str)}")
        
        else:
            print("Please specify --setup, --categorize-all, --email-id, or --summary")
    
    finally:
        categorizer.close_database()

if __name__ == "__main__":
    main()
