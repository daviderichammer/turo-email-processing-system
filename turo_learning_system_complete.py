#!/usr/bin/python3
"""
Turo Email Categorization Learning System - COMPLETE FIXED VERSION
Shows ALL patterns for selection, not just some of them
"""

import mysql.connector
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'SecureRootPass123!',
    'database': 'email_server',
    'charset': 'utf8mb4'
}

class TuroLearningSystemComplete:
    def __init__(self):
        self.db_connection = None
        
    def connect_database(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def close_database(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
            print("✅ Database connection closed")
    
    def create_categorization_rules_table(self):
        """Create the missing categorization_rules table"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS categorization_rules (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    category_name VARCHAR(100) NOT NULL,
                    rule_type ENUM('regex', 'keyword', 'pattern') DEFAULT 'regex',
                    field_name ENUM('subject', 'body', 'sender', 'combined') DEFAULT 'subject',
                    pattern TEXT NOT NULL,
                    confidence_score DECIMAL(3,2) DEFAULT 0.85,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_by VARCHAR(50) DEFAULT 'learning_system',
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP NULL,
                    usage_count INT DEFAULT 0,
                    INDEX idx_category (category_name),
                    INDEX idx_active (is_active),
                    INDEX idx_field (field_name)
                )
            """)
            self.db_connection.commit()
            print("✅ categorization_rules table created/verified")
        except Exception as e:
            print(f"❌ Error creating categorization_rules table: {e}")
        finally:
            cursor.close()
    
    def show_uncategorized_emails(self, limit: int = 10) -> List[Dict]:
        """Show uncategorized emails for human review"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT 
                    id, 
                    LEFT(subject, 80) as subject,
                    LEFT(body_text, 200) as body_preview,
                    sender_email,
                    received_date
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
                AND is_duplicate = FALSE
                AND category_id IS NULL
                ORDER BY received_date DESC
                LIMIT %s
            """, (limit,))
            
            emails = cursor.fetchall()
            
            print(f"\n📧 UNCATEGORIZED EMAILS ({len(emails)} shown):")
            print("=" * 80)
            
            for i, email in enumerate(emails, 1):
                print(f"\n{i}. ID: {email['id']}")
                print(f"   Subject: {email['subject']}")
                print(f"   Preview: {email['body_preview']}...")
                print(f"   Date: {email['received_date']}")
            
            return emails
            
        except Exception as e:
            print(f"❌ Error fetching uncategorized emails: {e}")
            return []
        finally:
            cursor.close()
    
    def show_categories(self) -> Dict[str, int]:
        """Show available categories"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, name, description 
                FROM email_categories 
                WHERE is_active = TRUE 
                ORDER BY name
            """)
            
            categories = cursor.fetchall()
            
            print(f"\n📋 AVAILABLE CATEGORIES:")
            print("=" * 50)
            
            category_map = {}
            for i, cat in enumerate(categories, 1):
                print(f"{i}. {cat['name']} - {cat['description']}")
                category_map[str(i)] = cat['id']
                category_map[cat['name']] = cat['id']
            
            return category_map
            
        except Exception as e:
            print(f"❌ Error fetching categories: {e}")
            return {}
        finally:
            cursor.close()
    
    def manually_categorize_email(self, email_id: int, category_id: int, confidence: int = 95) -> bool:
        """Manually assign an email to a category"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                UPDATE emails 
                SET category_id = %s, 
                    categorization_confidence = %s,
                    categorization_method = 'manual_assignment'
                WHERE id = %s
            """, (category_id, confidence, email_id))
            
            self.db_connection.commit()
            
            if cursor.rowcount > 0:
                print(f"✅ Email {email_id} categorized successfully")
                return True
            else:
                print(f"❌ Email {email_id} not found")
                return False
                
        except Exception as e:
            print(f"❌ Error categorizing email: {e}")
            return False
        finally:
            cursor.close()
    
    def extract_patterns_from_email(self, email_id: int) -> Dict:
        """Extract patterns from an email for rule creation"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT subject, body_text, sender_email
                FROM emails 
                WHERE id = %s
            """, (email_id,))
            
            email = cursor.fetchone()
            
            if not email:
                print(f"❌ Email {email_id} not found")
                return {}
            
            # Extract potential patterns
            patterns = {
                'subject_keywords': self.extract_keywords(email['subject']),
                'body_keywords': self.extract_keywords(email['body_text']),
                'subject_patterns': self.suggest_regex_patterns(email['subject']),
                'body_patterns': self.suggest_regex_patterns(email['body_text'])
            }
            
            return patterns
            
        except Exception as e:
            print(f"❌ Error extracting patterns: {e}")
            return {}
        finally:
            cursor.close()
    
    def extract_keywords(self, text: str) -> List[str]:
        """Extract meaningful keywords from text"""
        if not text:
            return []
        
        # Remove common words and extract meaningful terms
        common_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'a', 'an', 'is', 'are', 'was', 'were', 'has', 'have', 'had', 'you', 'your', 'their', 'this', 'that', 'will', 'can', 'about', 'from', 'they', 'them', 'be', 'been', 'being'}
        
        # Extract words
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        keywords = [word for word in words if word not in common_words]
        
        # Get unique keywords, sorted by frequency
        keyword_freq = {}
        for word in keywords:
            keyword_freq[word] = keyword_freq.get(word, 0) + 1
        
        return sorted(keyword_freq.keys(), key=lambda x: keyword_freq[x], reverse=True)[:8]
    
    def suggest_regex_patterns(self, text: str) -> List[str]:
        """Suggest regex patterns based on text content"""
        if not text:
            return []
        
        patterns = []
        text_lower = text.lower()
        
        # Common Turo patterns
        if 'has sent you a message' in text_lower:
            patterns.append(r'.*has sent you a message.*')
        
        if 'booked' in text_lower or 'booking' in text_lower:
            patterns.append(r'.*(booked|booking).*')
        
        if 'returned' in text_lower or 'return' in text_lower:
            patterns.append(r'.*(returned|return).*')
        
        if 'rate' in text_lower or 'review' in text_lower:
            patterns.append(r'.*(rate|review|rating).*')
        
        if 'changed' in text_lower or 'modify' in text_lower or 'modification' in text_lower:
            patterns.append(r'.*(changed|change|modified|modification).*')
        
        if 'payment' in text_lower or 'payout' in text_lower:
            patterns.append(r'.*(payment|payout|earnings).*')
        
        if 'cancel' in text_lower:
            patterns.append(r'.*(cancel|cancelled|cancellation).*')
        
        if 'insurance' in text_lower or 'claim' in text_lower or 'damage' in text_lower:
            patterns.append(r'.*(insurance|claim|damage).*')
        
        if 'reminder' in text_lower or 'remind' in text_lower:
            patterns.append(r'.*(reminder|remind).*')
        
        if 'upcoming' in text_lower:
            patterns.append(r'.*upcoming.*')
        
        return patterns
    
    def create_new_category(self, name: str, description: str) -> int:
        """Create a new category"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                INSERT INTO email_categories (name, description, confidence_threshold, auto_assign, is_active)
                VALUES (%s, %s, 0.85, TRUE, TRUE)
            """, (name, description))
            
            category_id = cursor.lastrowid
            self.db_connection.commit()
            
            print(f"✅ New category '{name}' created with ID: {category_id}")
            return category_id
            
        except Exception as e:
            print(f"❌ Error creating category: {e}")
            return None
        finally:
            cursor.close()
    
    def add_categorization_rule(self, category_name: str, rule_data: Dict) -> bool:
        """Add a new categorization rule based on learned patterns"""
        try:
            cursor = self.db_connection.cursor()
            
            # Insert rule into categorization_rules table
            cursor.execute("""
                INSERT INTO categorization_rules (
                    category_name, rule_type, field_name, pattern, 
                    confidence_score, is_active, created_by
                ) VALUES (%s, %s, %s, %s, %s, TRUE, 'learning_system')
            """, (
                category_name,
                rule_data.get('type', 'regex'),
                rule_data.get('field', 'subject'),
                rule_data.get('pattern', ''),
                rule_data.get('confidence', 0.85)
            ))
            
            self.db_connection.commit()
            print(f"✅ Added rule: {rule_data.get('field', 'subject')} - {rule_data.get('pattern', '')}")
            return True
            
        except Exception as e:
            print(f"❌ Error adding rule: {e}")
            return False
        finally:
            cursor.close()
    
    def learn_from_manual_categorization(self, email_id: int, category_name: str) -> bool:
        """Learn patterns from manual categorization - COMPLETE FIXED VERSION"""
        try:
            # Extract patterns from the email
            patterns = self.extract_patterns_from_email(email_id)
            
            if not patterns:
                return False
            
            print(f"\n🧠 LEARNING FROM EMAIL {email_id}:")
            print("=" * 50)
            
            # Show suggested patterns
            print(f"📝 Suggested patterns for '{category_name}':")
            
            # BUILD COMPLETE LIST OF ALL POSSIBLE RULES
            suggested_rules = []
            
            # Add subject regex patterns
            if patterns['subject_patterns']:
                print(f"   Subject patterns: {patterns['subject_patterns']}")
                for pattern in patterns['subject_patterns']:
                    suggested_rules.append({
                        'type': 'regex',
                        'field': 'subject',
                        'pattern': pattern,
                        'confidence': 0.90,
                        'description': f"subject regex: {pattern}"
                    })
            
            # Add body regex patterns
            if patterns['body_patterns']:
                print(f"   Body patterns: {patterns['body_patterns']}")
                for pattern in patterns['body_patterns']:
                    suggested_rules.append({
                        'type': 'regex',
                        'field': 'body',
                        'pattern': pattern,
                        'confidence': 0.85,
                        'description': f"body regex: {pattern}"
                    })
            
            # Add subject keyword patterns
            if patterns['subject_keywords']:
                print(f"   Subject keywords: {patterns['subject_keywords']}")
                for keyword in patterns['subject_keywords'][:5]:  # Top 5 keywords
                    suggested_rules.append({
                        'type': 'keyword',
                        'field': 'subject',
                        'pattern': f'.*{re.escape(keyword)}.*',
                        'confidence': 0.80,
                        'description': f"subject keyword: {keyword}"
                    })
            
            # Add body keyword patterns
            if patterns['body_keywords']:
                print(f"   Body keywords: {patterns['body_keywords']}")
                for keyword in patterns['body_keywords'][:5]:  # Top 5 keywords
                    suggested_rules.append({
                        'type': 'keyword',
                        'field': 'body',
                        'pattern': f'.*{re.escape(keyword)}.*',
                        'confidence': 0.75,
                        'description': f"body keyword: {keyword}"
                    })
            
            if not suggested_rules:
                print("❌ No patterns found to create rules")
                return False
            
            # FIXED: Actually wait for user input and show ALL patterns
            print(f"\n🤖 Would you like to add any of these as automatic rules for '{category_name}'?")
            print("This will help categorize similar emails automatically in the future.")
            print("\nOptions:")
            print("1. Add all suggested patterns")
            print("2. Select specific patterns")
            print("3. Skip adding rules")
            
            while True:
                choice = input("\nEnter your choice (1-3): ").strip()
                
                if choice == '1':
                    # Add all suggested rules
                    added_count = 0
                    for rule in suggested_rules:
                        if self.add_categorization_rule(category_name, rule):
                            added_count += 1
                    print(f"✅ Added {added_count} automatic rules for '{category_name}'")
                    break
                    
                elif choice == '2':
                    # FIXED: Show ALL patterns for selection, not just some
                    print(f"\nSelect patterns to add (enter numbers separated by commas):")
                    for i, rule in enumerate(suggested_rules, 1):
                        print(f"{i}. {rule['description']}")
                    
                    selection = input("Enter pattern numbers (e.g., 1,3,5): ").strip()
                    try:
                        selected_indices = [int(x.strip()) - 1 for x in selection.split(',') if x.strip().isdigit()]
                        added_count = 0
                        for idx in selected_indices:
                            if 0 <= idx < len(suggested_rules):
                                if self.add_categorization_rule(category_name, suggested_rules[idx]):
                                    added_count += 1
                        print(f"✅ Added {added_count} selected rules for '{category_name}'")
                        break
                    except ValueError:
                        print("❌ Invalid selection. Please enter numbers separated by commas.")
                        continue
                        
                elif choice == '3':
                    print("⏭️ Skipped adding automatic rules")
                    break
                    
                else:
                    print("❌ Invalid choice. Please enter 1, 2, or 3.")
                    continue
            
            return True
            
        except Exception as e:
            print(f"❌ Error learning from categorization: {e}")
            return False
    
    def show_learned_rules(self, category_name: str = None):
        """Show learned categorization rules"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            if category_name:
                cursor.execute("""
                    SELECT * FROM categorization_rules 
                    WHERE category_name = %s AND is_active = TRUE
                    ORDER BY confidence_score DESC
                """, (category_name,))
            else:
                cursor.execute("""
                    SELECT * FROM categorization_rules 
                    WHERE is_active = TRUE
                    ORDER BY category_name, confidence_score DESC
                """)
            
            rules = cursor.fetchall()
            
            if not rules:
                print(f"📝 No learned rules found" + (f" for '{category_name}'" if category_name else ""))
                return
            
            print(f"\n📚 LEARNED CATEGORIZATION RULES" + (f" FOR '{category_name}'" if category_name else ""))
            print("=" * 60)
            
            current_category = None
            for rule in rules:
                if rule['category_name'] != current_category:
                    current_category = rule['category_name']
                    print(f"\n🏷️  {current_category.upper()}")
                    print("-" * 40)
                
                print(f"   {rule['field_name']}: {rule['pattern']}")
                print(f"   Type: {rule['rule_type']}, Confidence: {rule['confidence_score']}, Used: {rule['usage_count']} times")
                print()
            
        except Exception as e:
            print(f"❌ Error showing learned rules: {e}")
        finally:
            cursor.close()
    
    def interactive_learning_session(self):
        """Interactive session for teaching the system - COMPLETE FIXED VERSION"""
        print("\n🎓 TURO EMAIL CATEGORIZATION LEARNING SYSTEM (COMPLETE)")
        print("=" * 60)
        print("This system helps you teach the categorization engine new patterns.")
        print("✅ FIXED: Now shows ALL patterns for selection!")
        
        # Ensure the categorization_rules table exists
        self.create_categorization_rules_table()
        
        while True:
            print("\n📋 LEARNING OPTIONS:")
            print("1. Show uncategorized emails")
            print("2. Manually categorize an email")
            print("3. Create new category")
            print("4. Show categories")
            print("5. Show learned rules")
            print("6. Exit")
            
            choice = input("\nSelect option (1-6): ").strip()
            
            if choice == '1':
                limit = input("How many emails to show? (default 10): ").strip()
                limit = int(limit) if limit.isdigit() else 10
                self.show_uncategorized_emails(limit)
                
            elif choice == '2':
                email_id = input("Enter email ID to categorize: ").strip()
                if not email_id.isdigit():
                    print("❌ Invalid email ID")
                    continue
                
                categories = self.show_categories()
                category_choice = input("Enter category number or name: ").strip()
                
                if category_choice in categories:
                    category_id = categories[category_choice]
                    success = self.manually_categorize_email(int(email_id), category_id)
                    
                    if success:
                        # Learn from this categorization - COMPLETE FIXED VERSION
                        category_name = next(name for name, id in categories.items() if id == category_id and not name.isdigit())
                        self.learn_from_manual_categorization(int(email_id), category_name)
                else:
                    print("❌ Invalid category selection")
                    
            elif choice == '3':
                name = input("Enter new category name: ").strip()
                description = input("Enter category description: ").strip()
                
                if name and description:
                    self.create_new_category(name, description)
                else:
                    print("❌ Name and description are required")
                    
            elif choice == '4':
                self.show_categories()
                
            elif choice == '5':
                category_name = input("Enter category name (or press Enter for all): ").strip()
                category_name = category_name if category_name else None
                self.show_learned_rules(category_name)
                
            elif choice == '6':
                print("👋 Exiting learning system")
                break
                
            else:
                print("❌ Invalid choice")

def main():
    """Main execution function"""
    import sys
    
    learning_system = TuroLearningSystemComplete()
    learning_system.connect_database()
    
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == 'uncategorized':
                limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
                learning_system.show_uncategorized_emails(limit)
                
            elif command == 'categories':
                learning_system.show_categories()
                
            elif command == 'rules':
                category_name = sys.argv[2] if len(sys.argv) > 2 else None
                learning_system.show_learned_rules(category_name)
                
            elif command == 'categorize':
                if len(sys.argv) < 4:
                    print("Usage: python3 turo_learning_system_complete.py categorize <email_id> <category_name>")
                    return
                
                email_id = int(sys.argv[2])
                category_name = sys.argv[3]
                
                categories = learning_system.show_categories()
                if category_name in categories:
                    category_id = categories[category_name]
                    learning_system.manually_categorize_email(email_id, category_id)
                    learning_system.learn_from_manual_categorization(email_id, category_name)
                else:
                    print(f"❌ Category '{category_name}' not found")
                    
            elif command == 'interactive':
                learning_system.interactive_learning_session()
                
            else:
                print("❌ Unknown command")
                print("Available commands: uncategorized, categories, rules, categorize, interactive")
        else:
            # Default to interactive mode
            learning_system.interactive_learning_session()
            
    finally:
        learning_system.close_database()

if __name__ == "__main__":
    main()
