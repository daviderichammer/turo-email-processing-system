#!/usr/bin/python3
"""
Turo Category Management Interface
Provides human oversight and intervention capabilities for email categorization
"""

import sys
import json
import mysql.connector
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from tabulate import tabulate

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'SecureRootPass123!',
    'database': 'email_server',
    'charset': 'utf8mb4'
}

class TuroCategoryManager:
    def __init__(self):
        self.db_connection = None
        
    def connect_database(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def close_database(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()
    
    def list_categories(self):
        """List all categories with statistics"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    ec.id,
                    ec.category_name,
                    ec.description,
                    ec.auto_assign,
                    ec.confidence_threshold,
                    COUNT(eca.email_id) as email_count,
                    AVG(eca.confidence_score) as avg_confidence,
                    ec.created_at
                FROM email_categories ec
                LEFT JOIN email_category_assignments eca ON ec.id = eca.category_id
                GROUP BY ec.id
                ORDER BY email_count DESC, ec.category_name
            """
            
            cursor.execute(query)
            categories = cursor.fetchall()
            
            if not categories:
                print("📭 No categories found")
                return
            
            print(f"📋 Email Categories ({len(categories)} total)")
            print("=" * 100)
            
            table_data = []
            for cat in categories:
                status = "🟢 Auto" if cat['auto_assign'] else "🔴 Manual"
                table_data.append([
                    cat['id'],
                    cat['category_name'],
                    cat['email_count'] or 0,
                    f"{cat['avg_confidence']:.2f}" if cat['avg_confidence'] else "N/A",
                    f"{cat['confidence_threshold']:.2f}",
                    status,
                    cat['created_at'].strftime('%Y-%m-%d')
                ])
            
            headers = ['ID', 'Category Name', 'Emails', 'Avg Confidence', 'Threshold', 'Status', 'Created']
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
            
        except Exception as e:
            print(f"❌ Failed to list categories: {e}")
        finally:
            cursor.close()
    
    def show_category_details(self, category_id: int):
        """Show detailed information about a category"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get category info
            cursor.execute("SELECT * FROM email_categories WHERE id = %s", (category_id,))
            category = cursor.fetchone()
            
            if not category:
                print(f"❌ Category #{category_id} not found")
                return
            
            print(f"🔧 Category #{category['id']}: {category['category_name']}")
            print("=" * 60)
            
            status = "🟢 Auto-assign" if category['auto_assign'] else "🔴 Manual only"
            print(f"Status: {status}")
            print(f"Description: {category['description']}")
            print(f"Confidence Threshold: {category['confidence_threshold']}")
            print(f"Created: {category['created_at']}")
            
            # Show patterns
            print("\n📋 Matching Patterns:")
            cursor.execute("""
                SELECT pattern_type, pattern_regex, pattern_weight, success_rate, usage_count, is_active
                FROM category_patterns 
                WHERE category_id = %s 
                ORDER BY pattern_weight DESC, success_rate DESC
            """, (category_id,))
            patterns = cursor.fetchall()
            
            if patterns:
                pattern_table = []
                for pattern in patterns:
                    active = "✅" if pattern['is_active'] else "❌"
                    pattern_table.append([
                        pattern['pattern_type'],
                        pattern['pattern_regex'][:50] + "..." if len(pattern['pattern_regex']) > 50 else pattern['pattern_regex'],
                        f"{pattern['pattern_weight']:.2f}",
                        f"{pattern['success_rate']:.2f}" if pattern['success_rate'] else "N/A",
                        pattern['usage_count'] or 0,
                        active
                    ])
                
                headers = ['Type', 'Pattern', 'Weight', 'Success Rate', 'Usage', 'Active']
                print(tabulate(pattern_table, headers=headers, tablefmt='grid'))
            else:
                print("  No patterns configured")
            
            # Show recent emails
            print(f"\n📧 Recent Emails (Last 10):")
            cursor.execute("""
                SELECT e.id, e.subject, eca.confidence_score, eca.assignment_method, eca.assigned_at
                FROM emails e
                JOIN email_category_assignments eca ON e.id = eca.email_id
                WHERE eca.category_id = %s
                ORDER BY eca.assigned_at DESC
                LIMIT 10
            """, (category_id,))
            emails = cursor.fetchall()
            
            if emails:
                email_table = []
                for email in emails:
                    email_table.append([
                        email['id'],
                        email['subject'][:60] + "..." if len(email['subject']) > 60 else email['subject'],
                        f"{email['confidence_score']:.2f}",
                        email['assignment_method'],
                        email['assigned_at'].strftime('%Y-%m-%d %H:%M')
                    ])
                
                headers = ['Email ID', 'Subject', 'Confidence', 'Method', 'Assigned']
                print(tabulate(email_table, headers=headers, tablefmt='grid'))
            else:
                print("  No emails assigned to this category")
            
        except Exception as e:
            print(f"❌ Failed to show category details: {e}")
        finally:
            cursor.close()
    
    def create_category(self, name: str, description: str, confidence_threshold: float = 0.80, auto_assign: bool = True):
        """Create a new category"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO email_categories (category_name, description, confidence_threshold, auto_assign)
                VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(query, (name, description, confidence_threshold, auto_assign))
            category_id = cursor.lastrowid
            self.db_connection.commit()
            
            print(f"✅ Created category #{category_id}: {name}")
            return category_id
            
        except Exception as e:
            print(f"❌ Failed to create category: {e}")
            return None
        finally:
            cursor.close()
    
    def merge_categories(self, source_category_id: int, target_category_id: int, delete_source: bool = True):
        """Merge one category into another"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get category info
            cursor.execute("SELECT category_name FROM email_categories WHERE id = %s", (source_category_id,))
            source_cat = cursor.fetchone()
            cursor.execute("SELECT category_name FROM email_categories WHERE id = %s", (target_category_id,))
            target_cat = cursor.fetchone()
            
            if not source_cat or not target_cat:
                print("❌ One or both categories not found")
                return False
            
            print(f"🔄 Merging '{source_cat['category_name']}' into '{target_cat['category_name']}'")
            
            # Move email assignments
            cursor.execute("""
                UPDATE email_category_assignments 
                SET category_id = %s, assignment_method = 'manual'
                WHERE category_id = %s
            """, (target_category_id, source_category_id))
            
            moved_assignments = cursor.rowcount
            
            # Move patterns
            cursor.execute("""
                UPDATE category_patterns 
                SET category_id = %s
                WHERE category_id = %s
            """, (target_category_id, source_category_id))
            
            moved_patterns = cursor.rowcount
            
            # Delete source category if requested
            if delete_source:
                cursor.execute("DELETE FROM email_categories WHERE id = %s", (source_category_id,))
            else:
                cursor.execute("UPDATE email_categories SET auto_assign = FALSE WHERE id = %s", (source_category_id,))
            
            self.db_connection.commit()
            
            print(f"✅ Merge completed:")
            print(f"  • Moved {moved_assignments} email assignments")
            print(f"  • Moved {moved_patterns} patterns")
            if delete_source:
                print(f"  • Deleted source category")
            else:
                print(f"  • Disabled source category")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to merge categories: {e}")
            return False
        finally:
            cursor.close()
    
    def split_category(self, source_category_id: int, new_category_name: str, 
                      new_category_description: str, email_ids: List[int]):
        """Split emails from one category into a new category"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get source category info
            cursor.execute("SELECT category_name FROM email_categories WHERE id = %s", (source_category_id,))
            source_cat = cursor.fetchone()
            
            if not source_cat:
                print(f"❌ Source category #{source_category_id} not found")
                return False
            
            # Create new category
            new_category_id = self.create_category(new_category_name, new_category_description)
            if not new_category_id:
                return False
            
            # Move specified emails to new category
            if email_ids:
                placeholders = ','.join(['%s'] * len(email_ids))
                query = f"""
                    UPDATE email_category_assignments 
                    SET category_id = %s, assignment_method = 'manual'
                    WHERE category_id = %s AND email_id IN ({placeholders})
                """
                
                cursor.execute(query, [new_category_id, source_category_id] + email_ids)
                moved_count = cursor.rowcount
                
                self.db_connection.commit()
                
                print(f"✅ Split completed:")
                print(f"  • Created new category #{new_category_id}: {new_category_name}")
                print(f"  • Moved {moved_count} emails from '{source_cat['category_name']}'")
                
                return True
            else:
                print("❌ No email IDs provided for split")
                return False
            
        except Exception as e:
            print(f"❌ Failed to split category: {e}")
            return False
        finally:
            cursor.close()
    
    def reassign_emails(self, email_ids: List[int], target_category_id: int):
        """Reassign emails to a different category"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get target category info
            cursor.execute("SELECT category_name FROM email_categories WHERE id = %s", (target_category_id,))
            target_cat = cursor.fetchone()
            
            if not target_cat:
                print(f"❌ Target category #{target_category_id} not found")
                return False
            
            # Reassign emails
            placeholders = ','.join(['%s'] * len(email_ids))
            query = f"""
                UPDATE email_category_assignments 
                SET category_id = %s, assignment_method = 'manual', assigned_at = CURRENT_TIMESTAMP
                WHERE email_id IN ({placeholders})
            """
            
            cursor.execute(query, [target_category_id] + email_ids)
            reassigned_count = cursor.rowcount
            
            self.db_connection.commit()
            
            print(f"✅ Reassigned {reassigned_count} emails to '{target_cat['category_name']}'")
            return True
            
        except Exception as e:
            print(f"❌ Failed to reassign emails: {e}")
            return False
        finally:
            cursor.close()
    
    def review_suggestions(self):
        """Review category suggestions from the system"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT id, suggested_name, description, suggestion_confidence, 
                       sample_email_ids, pattern_analysis, created_at
                FROM category_suggestions 
                WHERE status = 'pending'
                ORDER BY suggestion_confidence DESC, created_at DESC
            """)
            suggestions = cursor.fetchall()
            
            if not suggestions:
                print("📭 No pending category suggestions")
                return
            
            print(f"💡 Category Suggestions ({len(suggestions)} pending)")
            print("=" * 80)
            
            for suggestion in suggestions:
                print(f"\n🔍 Suggestion #{suggestion['id']}: {suggestion['suggested_name']}")
                print(f"   Description: {suggestion['description']}")
                print(f"   Confidence: {suggestion['suggestion_confidence']:.2f}")
                print(f"   Created: {suggestion['created_at']}")
                
                # Show sample emails
                sample_ids = json.loads(suggestion['sample_email_ids'])
                if sample_ids:
                    print(f"   Sample Emails: {len(sample_ids)} emails")
                    
                    # Get sample email subjects
                    placeholders = ','.join(['%s'] * len(sample_ids[:3]))  # Show first 3
                    cursor.execute(f"""
                        SELECT id, subject FROM emails 
                        WHERE id IN ({placeholders})
                    """, sample_ids[:3])
                    samples = cursor.fetchall()
                    
                    for sample in samples:
                        subject = sample['subject'][:60] + "..." if len(sample['subject']) > 60 else sample['subject']
                        print(f"     • #{sample['id']}: {subject}")
                
                # Show pattern analysis
                pattern_analysis = json.loads(suggestion['pattern_analysis'])
                if 'subject_patterns' in pattern_analysis:
                    print(f"   Detected Patterns:")
                    for pattern in pattern_analysis['subject_patterns'][:2]:  # Show first 2
                        pattern_short = pattern[:50] + "..." if len(pattern) > 50 else pattern
                        print(f"     • Subject: {pattern_short}")
            
        except Exception as e:
            print(f"❌ Failed to review suggestions: {e}")
        finally:
            cursor.close()
    
    def approve_suggestion(self, suggestion_id: int, category_name: str = None):
        """Approve a category suggestion and create the category"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get suggestion details
            cursor.execute("SELECT * FROM category_suggestions WHERE id = %s", (suggestion_id,))
            suggestion = cursor.fetchone()
            
            if not suggestion:
                print(f"❌ Suggestion #{suggestion_id} not found")
                return False
            
            # Use provided name or suggested name
            final_name = category_name or suggestion['suggested_name']
            
            # Create category
            category_id = self.create_category(
                final_name, 
                suggestion['description'], 
                suggestion['suggestion_confidence']
            )
            
            if category_id:
                # Assign sample emails to new category
                sample_ids = json.loads(suggestion['sample_email_ids'])
                if sample_ids:
                    self.reassign_emails(sample_ids, category_id)
                
                # Mark suggestion as approved
                cursor.execute("""
                    UPDATE category_suggestions 
                    SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (suggestion_id,))
                
                self.db_connection.commit()
                
                print(f"✅ Approved suggestion and created category #{category_id}: {final_name}")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ Failed to approve suggestion: {e}")
            return False
        finally:
            cursor.close()
    
    def reject_suggestion(self, suggestion_id: int, reason: str = ""):
        """Reject a category suggestion"""
        try:
            cursor = self.db_connection.cursor()
            
            cursor.execute("""
                UPDATE category_suggestions 
                SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (suggestion_id,))
            
            self.db_connection.commit()
            
            print(f"✅ Rejected suggestion #{suggestion_id}")
            if reason:
                print(f"   Reason: {reason}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to reject suggestion: {e}")
            return False
        finally:
            cursor.close()
    
    def show_uncategorized_emails(self, limit: int = 20):
        """Show emails that haven't been categorized"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT e.id, e.sender_email, e.subject, e.received_date
                FROM emails e
                LEFT JOIN email_category_assignments eca ON e.id = eca.email_id
                WHERE eca.email_id IS NULL 
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
                ORDER BY e.received_date DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            emails = cursor.fetchall()
            
            if not emails:
                print("✅ All emails have been categorized!")
                return
            
            print(f"📧 Uncategorized Emails ({len(emails)} shown, limit {limit})")
            print("=" * 100)
            
            table_data = []
            for email in emails:
                subject = email['subject'][:70] + "..." if len(email['subject']) > 70 else email['subject']
                table_data.append([
                    email['id'],
                    subject,
                    email['received_date'].strftime('%Y-%m-%d %H:%M')
                ])
            
            headers = ['Email ID', 'Subject', 'Received']
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
            
        except Exception as e:
            print(f"❌ Failed to show uncategorized emails: {e}")
        finally:
            cursor.close()
    
    def show_statistics(self):
        """Show categorization statistics"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            print("📊 Categorization Statistics")
            print("=" * 50)
            
            # Total emails
            cursor.execute("SELECT COUNT(*) as total FROM emails WHERE sender_email = 'noreply@mail.turo.com'")
            total_emails = cursor.fetchone()['total']
            
            # Categorized emails
            cursor.execute("""
                SELECT COUNT(DISTINCT eca.email_id) as categorized 
                FROM email_category_assignments eca
                JOIN emails e ON eca.email_id = e.id
                WHERE e.sender_email = 'noreply@mail.turo.com'
            """)
            categorized_emails = cursor.fetchone()['categorized']
            
            # Duplicates
            cursor.execute("SELECT COUNT(*) as duplicates FROM emails WHERE is_duplicate = TRUE")
            duplicate_emails = cursor.fetchone()['duplicates']
            
            # Pending suggestions
            cursor.execute("SELECT COUNT(*) as pending FROM category_suggestions WHERE status = 'pending'")
            pending_suggestions = cursor.fetchone()['pending']
            
            print(f"📧 Total Turo Emails: {total_emails}")
            print(f"✅ Categorized: {categorized_emails} ({categorized_emails/total_emails*100:.1f}%)")
            print(f"🔄 Duplicates: {duplicate_emails}")
            print(f"💡 Pending Suggestions: {pending_suggestions}")
            print(f"❓ Uncategorized: {total_emails - categorized_emails - duplicate_emails}")
            
            # Category breakdown
            print(f"\n📋 Category Breakdown:")
            cursor.execute("""
                SELECT ec.category_name, COUNT(eca.email_id) as count
                FROM email_categories ec
                LEFT JOIN email_category_assignments eca ON ec.id = eca.category_id
                LEFT JOIN emails e ON eca.email_id = e.id
                WHERE e.sender_email = 'noreply@mail.turo.com' OR e.sender_email IS NULL
                GROUP BY ec.id, ec.category_name
                ORDER BY count DESC
            """)
            categories = cursor.fetchall()
            
            for cat in categories:
                print(f"  • {cat['category_name']}: {cat['count'] or 0} emails")
            
        except Exception as e:
            print(f"❌ Failed to show statistics: {e}")
        finally:
            cursor.close()

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Turo Category Manager - Human oversight for email categorization")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List categories
    subparsers.add_parser('list', help='List all categories with statistics')
    
    # Show category details
    show_parser = subparsers.add_parser('show', help='Show detailed category information')
    show_parser.add_argument('category_id', type=int, help='Category ID to show')
    
    # Create category
    create_parser = subparsers.add_parser('create', help='Create a new category')
    create_parser.add_argument('name', help='Category name')
    create_parser.add_argument('description', help='Category description')
    create_parser.add_argument('--threshold', type=float, default=0.80, help='Confidence threshold')
    create_parser.add_argument('--manual', action='store_true', help='Manual assignment only')
    
    # Merge categories
    merge_parser = subparsers.add_parser('merge', help='Merge one category into another')
    merge_parser.add_argument('source_id', type=int, help='Source category ID')
    merge_parser.add_argument('target_id', type=int, help='Target category ID')
    merge_parser.add_argument('--keep-source', action='store_true', help='Keep source category (disable instead of delete)')
    
    # Split category
    split_parser = subparsers.add_parser('split', help='Split emails into a new category')
    split_parser.add_argument('source_id', type=int, help='Source category ID')
    split_parser.add_argument('new_name', help='New category name')
    split_parser.add_argument('new_description', help='New category description')
    split_parser.add_argument('email_ids', nargs='+', type=int, help='Email IDs to move')
    
    # Reassign emails
    reassign_parser = subparsers.add_parser('reassign', help='Reassign emails to different category')
    reassign_parser.add_argument('category_id', type=int, help='Target category ID')
    reassign_parser.add_argument('email_ids', nargs='+', type=int, help='Email IDs to reassign')
    
    # Review suggestions
    subparsers.add_parser('suggestions', help='Review pending category suggestions')
    
    # Approve suggestion
    approve_parser = subparsers.add_parser('approve', help='Approve a category suggestion')
    approve_parser.add_argument('suggestion_id', type=int, help='Suggestion ID')
    approve_parser.add_argument('--name', help='Override suggested category name')
    
    # Reject suggestion
    reject_parser = subparsers.add_parser('reject', help='Reject a category suggestion')
    reject_parser.add_argument('suggestion_id', type=int, help='Suggestion ID')
    reject_parser.add_argument('--reason', help='Rejection reason')
    
    # Show uncategorized
    uncategorized_parser = subparsers.add_parser('uncategorized', help='Show uncategorized emails')
    uncategorized_parser.add_argument('--limit', type=int, default=20, help='Number of emails to show')
    
    # Statistics
    subparsers.add_parser('stats', help='Show categorization statistics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize manager
    manager = TuroCategoryManager()
    manager.connect_database()
    
    try:
        if args.command == 'list':
            manager.list_categories()
        
        elif args.command == 'show':
            manager.show_category_details(args.category_id)
        
        elif args.command == 'create':
            manager.create_category(args.name, args.description, args.threshold, not args.manual)
        
        elif args.command == 'merge':
            manager.merge_categories(args.source_id, args.target_id, not args.keep_source)
        
        elif args.command == 'split':
            manager.split_category(args.source_id, args.new_name, args.new_description, args.email_ids)
        
        elif args.command == 'reassign':
            manager.reassign_emails(args.email_ids, args.category_id)
        
        elif args.command == 'suggestions':
            manager.review_suggestions()
        
        elif args.command == 'approve':
            manager.approve_suggestion(args.suggestion_id, args.name)
        
        elif args.command == 'reject':
            manager.reject_suggestion(args.suggestion_id, args.reason)
        
        elif args.command == 'uncategorized':
            manager.show_uncategorized_emails(args.limit)
        
        elif args.command == 'stats':
            manager.show_statistics()
    
    finally:
        manager.close_database()

if __name__ == "__main__":
    main()
