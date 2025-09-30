#!/usr/bin/env python3
"""
Email Server Administration Script
Provides easy management interface for the email server system
"""

import sys
import subprocess
import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime, timedelta

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'email_server',
    'user': 'root',
    'password': 'SecureRootPass123!'
}

class EmailServerAdmin:
    def __init__(self):
        self.db_connection = None
    
    def connect_db(self):
        """Connect to database"""
        try:
            self.db_connection = mysql.connector.connect(**DB_CONFIG)
            return True
        except Error as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
    
    def show_status(self):
        """Show email server status"""
        print("📊 Email Server Status")
        print("=" * 50)
        
        # Check Postfix status
        try:
            result = subprocess.run(['systemctl', 'is-active', 'postfix'], 
                                  capture_output=True, text=True)
            postfix_status = "✅ Running" if result.stdout.strip() == 'active' else "❌ Not Running"
        except:
            postfix_status = "❌ Unknown"
        
        print(f"Postfix Service: {postfix_status}")
        
        # Check MySQL status
        try:
            result = subprocess.run(['systemctl', 'is-active', 'mysql'], 
                                  capture_output=True, text=True)
            mysql_status = "✅ Running" if result.stdout.strip() == 'active' else "❌ Not Running"
        except:
            mysql_status = "❌ Unknown"
        
        print(f"MySQL Service: {mysql_status}")
        
        # Check port 25
        try:
            result = subprocess.run(['netstat', '-tlnp'], capture_output=True, text=True)
            port_25_open = "✅ Open" if ":25 " in result.stdout else "❌ Closed"
        except:
            port_25_open = "❌ Unknown"
        
        print(f"SMTP Port 25: {port_25_open}")
        
        # Database statistics
        if self.connect_db():
            try:
                cursor = self.db_connection.cursor()
                
                # Total emails
                cursor.execute("SELECT COUNT(*) FROM emails")
                total_emails = cursor.fetchone()[0]
                
                # Emails today
                cursor.execute("SELECT COUNT(*) FROM emails WHERE DATE(received_date) = CURDATE()")
                emails_today = cursor.fetchone()[0]
                
                # Processing status
                cursor.execute("""
                    SELECT processing_status, COUNT(*) 
                    FROM emails 
                    GROUP BY processing_status
                """)
                status_counts = dict(cursor.fetchall())
                
                print(f"\\nDatabase Statistics:")
                print(f"  Total Emails: {total_emails}")
                print(f"  Emails Today: {emails_today}")
                print(f"  Completed: {status_counts.get('completed', 0)}")
                print(f"  Failed: {status_counts.get('failed', 0)}")
                print(f"  Pending: {status_counts.get('pending', 0)}")
                
                cursor.close()
            except Error as e:
                print(f"❌ Database query failed: {e}")
            
            self.close_db()
    
    def list_emails(self, limit=10):
        """List recent emails"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, sender_email, recipient_email, subject, 
                       received_date, processing_status
                FROM emails 
                ORDER BY received_date DESC 
                LIMIT %s
            """, (limit,))
            
            emails = cursor.fetchall()
            
            print(f"📧 Recent Emails (Last {limit})")
            print("=" * 80)
            
            if not emails:
                print("No emails found.")
                return
            
            for email in emails:
                status_icon = {
                    'completed': '✅',
                    'failed': '❌',
                    'pending': '⏳',
                    'processing': '🔄'
                }.get(email['processing_status'], '❓')
                
                print(f"ID: {email['id']} {status_icon}")
                print(f"  From: {email['sender_email']}")
                print(f"  To: {email['recipient_email']}")
                print(f"  Subject: {email['subject'][:60]}{'...' if len(email['subject']) > 60 else ''}")
                print(f"  Date: {email['received_date']}")
                print(f"  Status: {email['processing_status']}")
                print("-" * 80)
            
            cursor.close()
        except Error as e:
            print(f"❌ Error listing emails: {e}")
        finally:
            self.close_db()
    
    def show_email(self, email_id):
        """Show detailed email information"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get email details
            cursor.execute("""
                SELECT * FROM emails WHERE id = %s
            """, (email_id,))
            
            email = cursor.fetchone()
            if not email:
                print(f"❌ Email with ID {email_id} not found.")
                return
            
            print(f"📧 Email Details (ID: {email_id})")
            print("=" * 60)
            print(f"Message ID: {email['message_id']}")
            print(f"From: {email['sender_name']} <{email['sender_email']}>")
            print(f"To: {email['recipient_name']} <{email['recipient_email']}>")
            print(f"Subject: {email['subject']}")
            print(f"Received: {email['received_date']}")
            print(f"Status: {email['processing_status']}")
            if email['processing_error']:
                print(f"Error: {email['processing_error']}")
            
            print("\\nBody (Text):")
            print("-" * 40)
            print(email['body_text'][:500] + ('...' if len(email['body_text']) > 500 else ''))
            
            # Get attachments
            cursor.execute("""
                SELECT filename, content_type, file_size, file_path
                FROM email_attachments 
                WHERE email_id = %s
            """, (email_id,))
            
            attachments = cursor.fetchall()
            if attachments:
                print("\\nAttachments:")
                print("-" * 40)
                for att in attachments:
                    print(f"  📎 {att['filename']} ({att['content_type']}, {att['file_size']} bytes)")
                    print(f"     Path: {att['file_path']}")
            
            cursor.close()
        except Error as e:
            print(f"❌ Error showing email: {e}")
        finally:
            self.close_db()
    
    def list_rules(self):
        """List processing rules"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, rule_name, description, condition_type, condition_value,
                       action_type, is_active, priority
                FROM processing_rules 
                ORDER BY priority ASC
            """)
            
            rules = cursor.fetchall()
            
            print("⚙️  Processing Rules")
            print("=" * 60)
            
            if not rules:
                print("No processing rules found.")
                return
            
            for rule in rules:
                status_icon = "✅" if rule['is_active'] else "❌"
                print(f"ID: {rule['id']} {status_icon} Priority: {rule['priority']}")
                print(f"  Name: {rule['rule_name']}")
                print(f"  Description: {rule['description']}")
                print(f"  Condition: {rule['condition_type']} = '{rule['condition_value']}'")
                print(f"  Action: {rule['action_type']}")
                print(f"  Active: {rule['is_active']}")
                print("-" * 60)
            
            cursor.close()
        except Error as e:
            print(f"❌ Error listing rules: {e}")
        finally:
            self.close_db()
    
    def add_rule(self, name, description, condition_type, condition_value, action_type, action_config, priority=0):
        """Add new processing rule"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                INSERT INTO processing_rules 
                (rule_name, description, condition_type, condition_value, 
                 action_type, action_config, priority, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            """, (name, description, condition_type, condition_value, 
                  action_type, json.dumps(action_config), priority))
            
            self.db_connection.commit()
            rule_id = cursor.lastrowid
            print(f"✅ Rule '{name}' added successfully (ID: {rule_id})")
            
            cursor.close()
        except Error as e:
            print(f"❌ Error adding rule: {e}")
        finally:
            self.close_db()
    
    def toggle_rule(self, rule_id):
        """Toggle rule active status"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                UPDATE processing_rules 
                SET is_active = NOT is_active 
                WHERE id = %s
            """, (rule_id,))
            
            if cursor.rowcount > 0:
                self.db_connection.commit()
                print(f"✅ Rule {rule_id} status toggled successfully")
            else:
                print(f"❌ Rule {rule_id} not found")
            
            cursor.close()
        except Error as e:
            print(f"❌ Error toggling rule: {e}")
        finally:
            self.close_db()
    
    def show_stats(self, days=7):
        """Show email statistics"""
        if not self.connect_db():
            return
        
        try:
            cursor = self.db_connection.cursor()
            
            print(f"📈 Email Statistics (Last {days} days)")
            print("=" * 50)
            
            # Daily email count
            cursor.execute("""
                SELECT DATE(received_date) as date, COUNT(*) as count
                FROM emails 
                WHERE received_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                GROUP BY DATE(received_date)
                ORDER BY date DESC
            """, (days,))
            
            daily_stats = cursor.fetchall()
            print("Daily Email Volume:")
            for date, count in daily_stats:
                print(f"  {date}: {count} emails")
            
            # Top senders
            cursor.execute("""
                SELECT sender_email, COUNT(*) as count
                FROM emails 
                WHERE received_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
                GROUP BY sender_email
                ORDER BY count DESC
                LIMIT 10
            """, (days,))
            
            top_senders = cursor.fetchall()
            print("\\nTop Senders:")
            for sender, count in top_senders:
                print(f"  {sender}: {count} emails")
            
            cursor.close()
        except Error as e:
            print(f"❌ Error showing statistics: {e}")
        finally:
            self.close_db()
    
    def test_processor(self):
        """Test email processor with sample email"""
        print("🧪 Testing Email Processor")
        print("=" * 30)
        
        test_email = """From: test@example.com
To: admin@doty.slicie.cloud
Subject: Test Email from Admin Script
Date: {}

This is a test email generated by the email server admin script.
It should be processed and stored in the database.

Test timestamp: {}
""".format(datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0000"), 
           datetime.now().isoformat())
        
        try:
            result = subprocess.run(
                ['python3', '/opt/email_processor.py'],
                input=test_email,
                text=True,
                capture_output=True
            )
            
            if result.returncode == 0:
                print("✅ Email processor test successful!")
                print("Check the database for the test email.")
            else:
                print("❌ Email processor test failed!")
                print(f"Error: {result.stderr}")
                
        except Exception as e:
            print(f"❌ Error running test: {e}")

def print_help():
    """Print help information"""
    print("""
📧 Email Server Administration Tool

Usage: python3 email_server_admin.py <command> [options]

Commands:
  status                    Show server status
  list [limit]             List recent emails (default: 10)
  show <email_id>          Show detailed email information
  rules                    List processing rules
  add-rule                 Add new processing rule (interactive)
  toggle-rule <rule_id>    Toggle rule active status
  stats [days]             Show statistics (default: 7 days)
  test                     Test email processor
  help                     Show this help message

Examples:
  python3 email_server_admin.py status
  python3 email_server_admin.py list 20
  python3 email_server_admin.py show 5
  python3 email_server_admin.py stats 30
  python3 email_server_admin.py toggle-rule 2
""")

def main():
    if len(sys.argv) < 2:
        print_help()
        return
    
    admin = EmailServerAdmin()
    command = sys.argv[1].lower()
    
    if command == 'status':
        admin.show_status()
    
    elif command == 'list':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        admin.list_emails(limit)
    
    elif command == 'show':
        if len(sys.argv) < 3:
            print("❌ Please provide email ID")
            return
        email_id = int(sys.argv[2])
        admin.show_email(email_id)
    
    elif command == 'rules':
        admin.list_rules()
    
    elif command == 'add-rule':
        print("Adding new processing rule...")
        name = input("Rule name: ")
        description = input("Description: ")
        condition_type = input("Condition type (sender/subject/body/attachment/custom): ")
        condition_value = input("Condition value: ")
        action_type = input("Action type (webhook/script/database/custom): ")
        
        action_config = {}
        if action_type == 'webhook':
            action_config['url'] = input("Webhook URL: ")
        elif action_type == 'script':
            action_config['script'] = input("Script name: ")
        
        priority = int(input("Priority (0-100): ") or "0")
        
        admin.add_rule(name, description, condition_type, condition_value, 
                      action_type, action_config, priority)
    
    elif command == 'toggle-rule':
        if len(sys.argv) < 3:
            print("❌ Please provide rule ID")
            return
        rule_id = int(sys.argv[2])
        admin.toggle_rule(rule_id)
    
    elif command == 'stats':
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        admin.show_stats(days)
    
    elif command == 'test':
        admin.test_processor()
    
    elif command == 'help':
        print_help()
    
    else:
        print(f"❌ Unknown command: {command}")
        print_help()

if __name__ == "__main__":
    main()
