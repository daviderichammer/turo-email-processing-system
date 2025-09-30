#!/usr/bin/python3
"""
Turo Category Management Dashboard
Comprehensive tool for managing email categories and generating business insights
"""

import mysql.connector
from datetime import datetime, timedelta
import json
import argparse
from typing import Dict, List, Optional
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
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TuroCategoryDashboard:
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
    
    def get_category_overview(self) -> Dict:
        """Get comprehensive category overview"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Get category breakdown
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN ec.name IS NULL THEN 'uncategorized'
                        ELSE ec.name
                    END as category,
                    COALESCE(ec.description, 'Emails without category') as description,
                    COUNT(*) as email_count,
                    ROUND(COUNT(*) * 100.0 / (
                        SELECT COUNT(*) FROM emails WHERE sender_email = 'noreply@mail.turo.com'
                    ), 1) as percentage,
                    ROUND(AVG(COALESCE(e.categorization_confidence, 0)), 1) as avg_confidence,
                    MIN(e.received_date) as earliest_email,
                    MAX(e.received_date) as latest_email
                FROM emails e
                LEFT JOIN email_categories ec ON e.category_id = ec.id
                WHERE e.sender_email = 'noreply@mail.turo.com'
                GROUP BY ec.id, ec.name, ec.description
                ORDER BY email_count DESC
            """)
            
            categories = cursor.fetchall()
            
            # Get totals
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_emails,
                    SUM(CASE WHEN category_id IS NOT NULL THEN 1 ELSE 0 END) as categorized_emails,
                    SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) as duplicate_emails,
                    COUNT(*) - SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) as unique_emails
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
            """)
            
            totals = cursor.fetchone()
            
            return {
                'totals': totals,
                'categories': categories,
                'categorization_rate': round((totals['categorized_emails'] / totals['total_emails']) * 100, 1) if totals['total_emails'] > 0 else 0
            }
            
        except Exception as e:
            logging.error(f"Error getting category overview: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
    
    def get_business_insights(self) -> Dict:
        """Generate business insights from categorized emails"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            insights = {}
            
            # Guest communication analysis
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_messages,
                    COUNT(DISTINCT DATE(received_date)) as active_days,
                    ROUND(COUNT(*) / COUNT(DISTINCT DATE(received_date)), 1) as avg_messages_per_day
                FROM emails e
                JOIN email_categories ec ON e.category_id = ec.id
                WHERE ec.name = 'guest_messages' 
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
            """)
            
            guest_stats = cursor.fetchone()
            insights['guest_communication'] = guest_stats
            
            # Booking analysis
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_bookings,
                    COUNT(DISTINCT DATE(received_date)) as booking_days,
                    ROUND(COUNT(*) / COUNT(DISTINCT DATE(received_date)), 1) as avg_bookings_per_day
                FROM emails e
                JOIN email_categories ec ON e.category_id = ec.id
                WHERE ec.name = 'trip_bookings' 
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
            """)
            
            booking_stats = cursor.fetchone()
            insights['booking_activity'] = booking_stats
            
            # Vehicle return analysis
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_returns,
                    COUNT(DISTINCT DATE(received_date)) as return_days,
                    ROUND(COUNT(*) / COUNT(DISTINCT DATE(received_date)), 1) as avg_returns_per_day
                FROM emails e
                JOIN email_categories ec ON e.category_id = ec.id
                WHERE ec.name = 'vehicle_returns' 
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
            """)
            
            return_stats = cursor.fetchone()
            insights['return_activity'] = return_stats
            
            # Review activity
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_reviews,
                    COUNT(DISTINCT DATE(received_date)) as review_days,
                    ROUND(COUNT(*) / COUNT(DISTINCT DATE(received_date)), 1) as avg_reviews_per_day
                FROM emails e
                JOIN email_categories ec ON e.category_id = ec.id
                WHERE ec.name = 'ratings_reviews' 
                AND e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
            """)
            
            review_stats = cursor.fetchone()
            insights['review_activity'] = review_stats
            
            # Recent activity (last 7 days)
            cursor.execute("""
                SELECT 
                    ec.name as category,
                    COUNT(*) as count
                FROM emails e
                JOIN email_categories ec ON e.category_id = ec.id
                WHERE e.sender_email = 'noreply@mail.turo.com'
                AND e.is_duplicate = FALSE
                AND e.received_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY ec.name
                ORDER BY count DESC
            """)
            
            recent_activity = cursor.fetchall()
            insights['recent_activity'] = recent_activity
            
            return insights
            
        except Exception as e:
            logging.error(f"Error generating business insights: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
    
    def get_duplicate_analysis(self) -> Dict:
        """Analyze duplicate detection results"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            # Overall duplicate stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_emails,
                    SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) as duplicates,
                    COUNT(*) - SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) as unique_emails,
                    ROUND(SUM(CASE WHEN is_duplicate = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as duplicate_percentage
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
            """)
            
            duplicate_stats = cursor.fetchone()
            
            # Duplicate detection methods
            cursor.execute("""
                SELECT 
                    detection_method,
                    COUNT(*) as count,
                    ROUND(AVG(similarity_score), 3) as avg_similarity
                FROM email_duplicates
                GROUP BY detection_method
                ORDER BY count DESC
            """)
            
            detection_methods = cursor.fetchall()
            
            return {
                'overall_stats': duplicate_stats,
                'detection_methods': detection_methods
            }
            
        except Exception as e:
            logging.error(f"Error analyzing duplicates: {e}")
            return {'error': str(e)}
        finally:
            cursor.close()
    
    def get_uncategorized_emails(self, limit: int = 20) -> List[Dict]:
        """Get sample of uncategorized emails for review"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT 
                    id,
                    LEFT(subject, 80) as subject,
                    LEFT(body_text, 200) as body_preview,
                    received_date,
                    is_duplicate
                FROM emails 
                WHERE sender_email = 'noreply@mail.turo.com'
                AND category_id IS NULL
                AND is_duplicate = FALSE
                ORDER BY received_date DESC
                LIMIT %s
            """, (limit,))
            
            return cursor.fetchall()
            
        except Exception as e:
            logging.error(f"Error getting uncategorized emails: {e}")
            return []
        finally:
            cursor.close()
    
    def export_category_data(self, category_name: str, output_file: str = None) -> str:
        """Export emails from a specific category"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            if category_name == 'uncategorized':
                cursor.execute("""
                    SELECT 
                        id, subject, body_text, received_date, sender_email
                    FROM emails 
                    WHERE sender_email = 'noreply@mail.turo.com'
                    AND category_id IS NULL
                    AND is_duplicate = FALSE
                    ORDER BY received_date DESC
                """)
            elif category_name == 'duplicates':
                cursor.execute("""
                    SELECT 
                        id, subject, body_text, received_date, sender_email
                    FROM emails 
                    WHERE sender_email = 'noreply@mail.turo.com'
                    AND is_duplicate = TRUE
                    ORDER BY received_date DESC
                """)
            else:
                cursor.execute("""
                    SELECT 
                        e.id, e.subject, e.body_text, e.received_date, e.sender_email,
                        e.categorization_confidence, e.categorization_method
                    FROM emails e
                    JOIN email_categories ec ON e.category_id = ec.id
                    WHERE ec.name = %s
                    AND e.sender_email = 'noreply@mail.turo.com'
                    AND e.is_duplicate = FALSE
                    ORDER BY e.received_date DESC
                """, (category_name,))
            
            emails = cursor.fetchall()
            
            if not output_file:
                output_file = f"/tmp/turo_{category_name}_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(output_file, 'w') as f:
                json.dump(emails, f, indent=2, default=str)
            
            return output_file
            
        except Exception as e:
            logging.error(f"Error exporting category data: {e}")
            return None
        finally:
            cursor.close()
    
    def print_dashboard(self):
        """Print comprehensive dashboard"""
        print("🎯 TURO EMAIL CATEGORIZATION DASHBOARD")
        print("=" * 60)
        
        # Category Overview
        overview = self.get_category_overview()
        if 'error' not in overview:
            print(f"\n📊 OVERVIEW")
            print(f"Total Emails: {overview['totals']['total_emails']}")
            print(f"Unique Emails: {overview['totals']['unique_emails']}")
            print(f"Duplicate Emails: {overview['totals']['duplicate_emails']}")
            print(f"Categorized: {overview['totals']['categorized_emails']} ({overview['categorization_rate']}%)")
            
            print(f"\n📋 CATEGORIES")
            for cat in overview['categories']:
                print(f"• {cat['category']}: {cat['email_count']} emails ({cat['percentage']}%) - Confidence: {cat['avg_confidence']}%")
        
        # Business Insights
        insights = self.get_business_insights()
        if 'error' not in insights:
            print(f"\n💼 BUSINESS INSIGHTS")
            
            if insights.get('guest_communication'):
                gc = insights['guest_communication']
                print(f"• Guest Messages: {gc['total_messages']} total, {gc['avg_messages_per_day']} per day avg")
            
            if insights.get('booking_activity'):
                ba = insights['booking_activity']
                print(f"• Bookings: {ba['total_bookings']} total, {ba['avg_bookings_per_day']} per day avg")
            
            if insights.get('return_activity'):
                ra = insights['return_activity']
                print(f"• Returns: {ra['total_returns']} total, {ra['avg_returns_per_day']} per day avg")
            
            if insights.get('review_activity'):
                rev = insights['review_activity']
                print(f"• Reviews: {rev['total_reviews']} total, {rev['avg_reviews_per_day']} per day avg")
        
        # Duplicate Analysis
        dup_analysis = self.get_duplicate_analysis()
        if 'error' not in dup_analysis:
            print(f"\n🔍 DUPLICATE DETECTION")
            stats = dup_analysis['overall_stats']
            print(f"• Duplicates Found: {stats['duplicates']} ({stats['duplicate_percentage']}%)")
            print(f"• Unique Emails: {stats['unique_emails']}")
            
            if dup_analysis['detection_methods']:
                print("• Detection Methods:")
                for method in dup_analysis['detection_methods']:
                    print(f"  - {method['detection_method']}: {method['count']} duplicates (avg similarity: {method['avg_similarity']})")
        
        # Uncategorized Sample
        uncategorized = self.get_uncategorized_emails(5)
        if uncategorized:
            print(f"\n❓ UNCATEGORIZED EMAILS (Sample)")
            for email in uncategorized:
                print(f"• ID {email['id']}: {email['subject'][:60]}...")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Turo Category Management Dashboard")
    parser.add_argument('--dashboard', action='store_true', help='Show full dashboard')
    parser.add_argument('--overview', action='store_true', help='Show category overview')
    parser.add_argument('--insights', action='store_true', help='Show business insights')
    parser.add_argument('--duplicates', action='store_true', help='Show duplicate analysis')
    parser.add_argument('--uncategorized', type=int, default=10, help='Show uncategorized emails')
    parser.add_argument('--export', type=str, help='Export category data (category name)')
    parser.add_argument('--output', type=str, help='Output file for export')
    
    args = parser.parse_args()
    
    dashboard = TuroCategoryDashboard()
    dashboard.connect_database()
    
    try:
        if args.dashboard:
            dashboard.print_dashboard()
        
        elif args.overview:
            overview = dashboard.get_category_overview()
            print(json.dumps(overview, indent=2, default=str))
        
        elif args.insights:
            insights = dashboard.get_business_insights()
            print(json.dumps(insights, indent=2, default=str))
        
        elif args.duplicates:
            dup_analysis = dashboard.get_duplicate_analysis()
            print(json.dumps(dup_analysis, indent=2, default=str))
        
        elif args.uncategorized:
            uncategorized = dashboard.get_uncategorized_emails(args.uncategorized)
            print(json.dumps(uncategorized, indent=2, default=str))
        
        elif args.export:
            output_file = dashboard.export_category_data(args.export, args.output)
            if output_file:
                print(f"Category '{args.export}' exported to: {output_file}")
            else:
                print(f"Failed to export category '{args.export}'")
        
        else:
            dashboard.print_dashboard()
    
    finally:
        dashboard.close_database()

if __name__ == "__main__":
    main()
