#!/usr/bin/python3
"""
Email Rule Manager - Management Interface for Enhanced Email Processing
Provides CLI and web interface for configuring regex rules, data extraction, database insertion, and HTTP API calls
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

class EmailRuleManager:
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
    
    def list_rules(self):
        """List all regex rules"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT id, name, description, active, priority, 
                       sender_pattern, subject_pattern, body_pattern,
                       extract_data, insert_to_database, make_http_call,
                       created_at
                FROM regex_rules 
                ORDER BY priority ASC, id ASC
            """
            
            cursor.execute(query)
            rules = cursor.fetchall()
            
            if not rules:
                print("📭 No regex rules found")
                return
            
            print(f"📋 Email Processing Rules ({len(rules)} total)")
            print("=" * 80)
            
            for rule in rules:
                status = "🟢 Active" if rule['active'] else "🔴 Inactive"
                print(f"\n🔧 Rule #{rule['id']}: {rule['name']} ({status})")
                print(f"   Priority: {rule['priority']}")
                print(f"   Description: {rule['description']}")
                
                # Patterns
                if rule['sender_pattern']:
                    print(f"   📧 Sender Pattern: {rule['sender_pattern']}")
                if rule['subject_pattern']:
                    print(f"   📝 Subject Pattern: {rule['subject_pattern']}")
                if rule['body_pattern']:
                    print(f"   📄 Body Pattern: {rule['body_pattern']}")
                
                # Actions
                actions = []
                if rule['extract_data']:
                    actions.append("🔍 Extract Data")
                if rule['insert_to_database']:
                    actions.append("💾 Database Insert")
                if rule['make_http_call']:
                    actions.append("🌐 HTTP Call")
                
                if actions:
                    print(f"   Actions: {', '.join(actions)}")
                
                print(f"   Created: {rule['created_at']}")
            
        except Exception as e:
            print(f"❌ Failed to list rules: {e}")
        finally:
            cursor.close()
    
    def show_rule_details(self, rule_id: int):
        """Show detailed information about a specific rule"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get rule details
            query = "SELECT * FROM regex_rules WHERE id = %s"
            cursor.execute(query, (rule_id,))
            rule = cursor.fetchone()
            
            if not rule:
                print(f"❌ Rule #{rule_id} not found")
                return
            
            print(f"🔧 Rule #{rule['id']}: {rule['name']}")
            print("=" * 60)
            
            # Basic info
            status = "🟢 Active" if rule['active'] else "🔴 Inactive"
            print(f"Status: {status}")
            print(f"Priority: {rule['priority']}")
            print(f"Description: {rule['description']}")
            print(f"Match Logic: {rule['match_logic']}")
            print(f"Created: {rule['created_at']}")
            
            # Patterns
            print("\n📋 Matching Patterns:")
            if rule['sender_pattern']:
                print(f"  📧 Sender: {rule['sender_pattern']}")
            if rule['subject_pattern']:
                print(f"  📝 Subject: {rule['subject_pattern']}")
            if rule['body_pattern']:
                print(f"  📄 Body: {rule['body_pattern']}")
            
            # Data extraction patterns
            if rule['extract_data']:
                print("\n🔍 Data Extraction Patterns:")
                query = "SELECT * FROM data_extraction_patterns WHERE rule_id = %s ORDER BY id"
                cursor.execute(query, (rule_id,))
                patterns = cursor.fetchall()
                
                if patterns:
                    for pattern in patterns:
                        required = "Required" if pattern['required'] else "Optional"
                        print(f"  • {pattern['field_name']} ({pattern['data_type']}, {required})")
                        print(f"    Source: {pattern['source_field']}")
                        print(f"    Pattern: {pattern['regex_pattern']}")
                        print(f"    Capture Group: {pattern['capture_group']}")
                else:
                    print("  No extraction patterns configured")
            
            # Database insertions
            if rule['insert_to_database']:
                print("\n💾 Database Insertions:")
                query = "SELECT * FROM database_insertions WHERE rule_id = %s ORDER BY id"
                cursor.execute(query, (rule_id,))
                insertions = cursor.fetchall()
                
                if insertions:
                    for insertion in insertions:
                        print(f"  • {insertion['target_database']}.{insertion['target_table']}")
                        print(f"    Description: {insertion['description']}")
                        
                        # Show field mappings
                        query2 = "SELECT * FROM database_field_mappings WHERE insertion_id = %s ORDER BY id"
                        cursor.execute(query2, (insertion['id'],))
                        mappings = cursor.fetchall()
                        
                        if mappings:
                            print("    Field Mappings:")
                            for mapping in mappings:
                                print(f"      {mapping['target_field']} ← {mapping['source_field']} ({mapping['source_type']})")
                else:
                    print("  No database insertions configured")
            
            # HTTP calls
            if rule['make_http_call']:
                print("\n🌐 HTTP API Calls:")
                query = "SELECT * FROM http_calls WHERE rule_id = %s ORDER BY id"
                cursor.execute(query, (rule_id,))
                http_calls = cursor.fetchall()
                
                if http_calls:
                    for call in http_calls:
                        status = "🟢 Active" if call['active'] else "🔴 Inactive"
                        print(f"  • {call['name']} ({status})")
                        print(f"    Method: {call['method']}")
                        print(f"    URL: {call['base_url']}")
                        print(f"    Auth: {call['auth_type']}")
                        print(f"    Retries: {call['max_retries']}")
                        
                        # Show parameters
                        query2 = "SELECT * FROM http_call_parameters WHERE http_call_id = %s ORDER BY id"
                        cursor.execute(query2, (call['id'],))
                        params = cursor.fetchall()
                        
                        if params:
                            print("    Parameters:")
                            for param in params:
                                print(f"      {param['parameter_type']}.{param['parameter_name']} ← {param['source_value']} ({param['source_type']})")
                else:
                    print("  No HTTP calls configured")
            
        except Exception as e:
            print(f"❌ Failed to show rule details: {e}")
        finally:
            cursor.close()
    
    def create_rule(self, name: str, description: str, sender_pattern: str = None, 
                   subject_pattern: str = None, body_pattern: str = None, 
                   match_logic: str = "AND", priority: int = 100):
        """Create a new regex rule"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO regex_rules 
                (name, description, sender_pattern, subject_pattern, body_pattern, 
                 match_logic, priority, active, extract_data, insert_to_database, make_http_call)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                name, description, sender_pattern, subject_pattern, body_pattern,
                match_logic, priority, True, False, False, False
            ))
            
            rule_id = cursor.lastrowid
            self.db_connection.commit()
            
            print(f"✅ Created rule #{rule_id}: {name}")
            return rule_id
            
        except Exception as e:
            print(f"❌ Failed to create rule: {e}")
            return None
        finally:
            cursor.close()
    
    def add_extraction_pattern(self, rule_id: int, field_name: str, source_field: str,
                              regex_pattern: str, capture_group: int = 1, 
                              data_type: str = "string", required: bool = False):
        """Add data extraction pattern to a rule"""
        try:
            cursor = self.db_connection.cursor()
            
            # Enable data extraction for the rule
            query = "UPDATE regex_rules SET extract_data = TRUE WHERE id = %s"
            cursor.execute(query, (rule_id,))
            
            # Add extraction pattern
            query = """
                INSERT INTO data_extraction_patterns 
                (rule_id, field_name, source_field, regex_pattern, capture_group, data_type, required)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                rule_id, field_name, source_field, regex_pattern, 
                capture_group, data_type, required
            ))
            
            self.db_connection.commit()
            print(f"✅ Added extraction pattern '{field_name}' to rule #{rule_id}")
            
        except Exception as e:
            print(f"❌ Failed to add extraction pattern: {e}")
        finally:
            cursor.close()
    
    def add_database_insertion(self, rule_id: int, target_database: str, target_table: str,
                              description: str = ""):
        """Add database insertion configuration to a rule"""
        try:
            cursor = self.db_connection.cursor()
            
            # Enable database insertion for the rule
            query = "UPDATE regex_rules SET insert_to_database = TRUE WHERE id = %s"
            cursor.execute(query, (rule_id,))
            
            # Add database insertion
            query = """
                INSERT INTO database_insertions 
                (rule_id, target_database, target_table, description)
                VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(query, (rule_id, target_database, target_table, description))
            insertion_id = cursor.lastrowid
            self.db_connection.commit()
            
            print(f"✅ Added database insertion to {target_database}.{target_table} for rule #{rule_id}")
            return insertion_id
            
        except Exception as e:
            print(f"❌ Failed to add database insertion: {e}")
            return None
        finally:
            cursor.close()
    
    def add_field_mapping(self, insertion_id: int, target_field: str, source_field: str,
                         source_type: str = "extracted_data", data_transformation: str = None):
        """Add field mapping to a database insertion"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO database_field_mappings 
                (insertion_id, target_field, source_field, source_type, data_transformation)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                insertion_id, target_field, source_field, source_type, data_transformation
            ))
            
            self.db_connection.commit()
            print(f"✅ Added field mapping: {target_field} ← {source_field}")
            
        except Exception as e:
            print(f"❌ Failed to add field mapping: {e}")
        finally:
            cursor.close()
    
    def add_http_call(self, rule_id: int, name: str, method: str, base_url: str,
                     auth_type: str = "none", auth_config: str = None,
                     headers: str = None, max_retries: int = 3, retry_delay: int = 5):
        """Add HTTP call configuration to a rule"""
        try:
            cursor = self.db_connection.cursor()
            
            # Enable HTTP calls for the rule
            query = "UPDATE regex_rules SET make_http_call = TRUE WHERE id = %s"
            cursor.execute(query, (rule_id,))
            
            # Add HTTP call
            query = """
                INSERT INTO http_calls 
                (rule_id, name, method, base_url, auth_type, auth_config, headers, 
                 max_retries, retry_delay, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                rule_id, name, method.upper(), base_url, auth_type, auth_config,
                headers, max_retries, retry_delay, True
            ))
            
            http_call_id = cursor.lastrowid
            self.db_connection.commit()
            
            print(f"✅ Added HTTP call '{name}' to rule #{rule_id}")
            return http_call_id
            
        except Exception as e:
            print(f"❌ Failed to add HTTP call: {e}")
            return None
        finally:
            cursor.close()
    
    def add_http_parameter(self, http_call_id: int, parameter_name: str, parameter_type: str,
                          source_type: str, source_value: str, data_transformation: str = None):
        """Add parameter to an HTTP call"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO http_call_parameters 
                (http_call_id, parameter_name, parameter_type, source_type, source_value, data_transformation)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                http_call_id, parameter_name, parameter_type, source_type, source_value, data_transformation
            ))
            
            self.db_connection.commit()
            print(f"✅ Added HTTP parameter: {parameter_type}.{parameter_name}")
            
        except Exception as e:
            print(f"❌ Failed to add HTTP parameter: {e}")
        finally:
            cursor.close()
    
    def toggle_rule(self, rule_id: int):
        """Toggle rule active status"""
        try:
            cursor = self.db_connection.cursor()
            
            # Get current status
            query = "SELECT active FROM regex_rules WHERE id = %s"
            cursor.execute(query, (rule_id,))
            result = cursor.fetchone()
            
            if not result:
                print(f"❌ Rule #{rule_id} not found")
                return
            
            new_status = not result[0]
            
            # Update status
            query = "UPDATE regex_rules SET active = %s WHERE id = %s"
            cursor.execute(query, (new_status, rule_id))
            self.db_connection.commit()
            
            status_text = "activated" if new_status else "deactivated"
            print(f"✅ Rule #{rule_id} {status_text}")
            
        except Exception as e:
            print(f"❌ Failed to toggle rule: {e}")
        finally:
            cursor.close()
    
    def test_rule(self, rule_id: int, test_email: str):
        """Test a rule against sample email content"""
        try:
            # Import the email processor for testing
            sys.path.append('/opt')
            from enhanced_email_processor_final import EnhancedEmailProcessor
            
            processor = EnhancedEmailProcessor()
            processor.connect_database()
            
            # Parse test email
            email_data = processor.parse_email(test_email)
            
            # Get rule
            cursor = self.db_connection.cursor(dictionary=True)
            query = "SELECT * FROM regex_rules WHERE id = %s"
            cursor.execute(query, (rule_id,))
            rule = cursor.fetchone()
            
            if not rule:
                print(f"❌ Rule #{rule_id} not found")
                return
            
            print(f"🧪 Testing Rule #{rule_id}: {rule['name']}")
            print("=" * 50)
            
            # Test pattern matching
            match_result = processor.check_rule_match(rule, email_data)
            
            if match_result:
                print("✅ Email matches rule patterns")
                
                # Test data extraction
                if rule['extract_data']:
                    print("\n🔍 Data Extraction Test:")
                    extracted = processor.extract_data_from_email(rule_id, email_data)
                    
                    if extracted:
                        for field, value in extracted.items():
                            print(f"  • {field}: {value}")
                    else:
                        print("  No data extracted")
                
            else:
                print("❌ Email does not match rule patterns")
            
            processor.close_database()
            
        except Exception as e:
            print(f"❌ Failed to test rule: {e}")
        finally:
            cursor.close()
    
    def show_statistics(self):
        """Show email processing statistics"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            print("📊 Email Processing Statistics")
            print("=" * 50)
            
            # Total emails
            query = "SELECT COUNT(*) as total FROM emails"
            cursor.execute(query)
            total_emails = cursor.fetchone()['total']
            print(f"📧 Total Emails: {total_emails}")
            
            # Processing status
            query = """
                SELECT processing_status, COUNT(*) as count 
                FROM emails 
                GROUP BY processing_status
            """
            cursor.execute(query)
            statuses = cursor.fetchall()
            
            print("\n📈 Processing Status:")
            for status in statuses:
                print(f"  • {status['processing_status']}: {status['count']}")
            
            # Rule executions
            query = """
                SELECT execution_type, status, COUNT(*) as count 
                FROM rule_executions 
                GROUP BY execution_type, status
                ORDER BY execution_type, status
            """
            cursor.execute(query)
            executions = cursor.fetchall()
            
            print("\n🔧 Rule Executions:")
            for execution in executions:
                print(f"  • {execution['execution_type']} ({execution['status']}): {execution['count']}")
            
            # Recent activity
            query = """
                SELECT DATE(created_at) as date, COUNT(*) as count 
                FROM emails 
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """
            cursor.execute(query)
            recent = cursor.fetchall()
            
            if recent:
                print("\n📅 Recent Activity (Last 7 days):")
                for day in recent:
                    print(f"  • {day['date']}: {day['count']} emails")
            
        except Exception as e:
            print(f"❌ Failed to show statistics: {e}")
        finally:
            cursor.close()

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Email Rule Manager - Configure email processing rules")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List rules
    subparsers.add_parser('list', help='List all regex rules')
    
    # Show rule details
    show_parser = subparsers.add_parser('show', help='Show detailed rule information')
    show_parser.add_argument('rule_id', type=int, help='Rule ID to show')
    
    # Create rule
    create_parser = subparsers.add_parser('create', help='Create a new regex rule')
    create_parser.add_argument('name', help='Rule name')
    create_parser.add_argument('description', help='Rule description')
    create_parser.add_argument('--sender', help='Sender pattern')
    create_parser.add_argument('--subject', help='Subject pattern')
    create_parser.add_argument('--body', help='Body pattern')
    create_parser.add_argument('--logic', choices=['AND', 'OR'], default='AND', help='Match logic')
    create_parser.add_argument('--priority', type=int, default=100, help='Rule priority')
    
    # Toggle rule
    toggle_parser = subparsers.add_parser('toggle', help='Toggle rule active status')
    toggle_parser.add_argument('rule_id', type=int, help='Rule ID to toggle')
    
    # Test rule
    test_parser = subparsers.add_parser('test', help='Test rule against sample email')
    test_parser.add_argument('rule_id', type=int, help='Rule ID to test')
    test_parser.add_argument('email_file', help='Path to test email file')
    
    # Statistics
    subparsers.add_parser('stats', help='Show processing statistics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize manager
    manager = EmailRuleManager()
    manager.connect_database()
    
    try:
        if args.command == 'list':
            manager.list_rules()
        
        elif args.command == 'show':
            manager.show_rule_details(args.rule_id)
        
        elif args.command == 'create':
            manager.create_rule(
                args.name, args.description, args.sender, 
                args.subject, args.body, args.logic, args.priority
            )
        
        elif args.command == 'toggle':
            manager.toggle_rule(args.rule_id)
        
        elif args.command == 'test':
            try:
                with open(args.email_file, 'r') as f:
                    test_email = f.read()
                manager.test_rule(args.rule_id, test_email)
            except FileNotFoundError:
                print(f"❌ Email file not found: {args.email_file}")
        
        elif args.command == 'stats':
            manager.show_statistics()
    
    finally:
        manager.close_database()

if __name__ == "__main__":
    main()
