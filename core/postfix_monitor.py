#!/usr/bin/env python3
"""
Postfix Email Monitor - Watch raw email input and processing
"""

import os
import sys
import time
import subprocess
import threading
from datetime import datetime
import re

class PostfixMonitor:
    def __init__(self):
        self.log_file = "/var/log/postfix.log"
        self.mail_log = "/var/log/mail.log"
        self.queue_dir = "/var/spool/postfix"
        
    def watch_logs(self):
        """Watch Postfix logs in real-time"""
        print("🔍 Watching Postfix logs in real-time...")
        print("=" * 60)
        
        try:
            # Use tail -f to follow the log file
            process = subprocess.Popen(
                ['tail', '-f', self.log_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            for line in process.stdout:
                timestamp = datetime.now().strftime("%H:%M:%S")
                if any(keyword in line.lower() for keyword in ['reademail', 'smtp', 'connect', 'disconnect', 'queue', 'error']):
                    print(f"[{timestamp}] {line.strip()}")
                    
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped")
            process.terminate()
        except Exception as e:
            print(f"❌ Error watching logs: {e}")
    
    def check_queue(self):
        """Check current mail queue status"""
        print("\n📬 Current Mail Queue Status:")
        print("-" * 40)
        
        try:
            result = subprocess.run(['postqueue', '-p'], capture_output=True, text=True)
            if result.stdout.strip():
                print(result.stdout)
            else:
                print("✅ Mail queue is empty")
        except Exception as e:
            print(f"❌ Error checking queue: {e}")
    
    def show_postfix_config(self):
        """Show relevant Postfix configuration"""
        print("\n⚙️ Postfix Configuration:")
        print("-" * 40)
        
        configs = [
            'mydestination',
            'virtual_mailbox_domains',
            'virtual_alias_maps',
            'transport_maps',
            'inet_interfaces',
            'mynetworks'
        ]
        
        for config in configs:
            try:
                result = subprocess.run(['postconf', config], capture_output=True, text=True)
                print(f"{config}: {result.stdout.strip()}")
            except Exception as e:
                print(f"❌ Error getting {config}: {e}")
    
    def test_smtp_connection(self):
        """Test SMTP connection"""
        print("\n🔌 Testing SMTP Connection:")
        print("-" * 40)
        
        try:
            result = subprocess.run(
                ['nc', '-v', 'localhost', '25'],
                input="QUIT\n",
                capture_output=True,
                text=True,
                timeout=5
            )
            print("✅ SMTP connection successful")
            print(f"Response: {result.stdout}")
        except Exception as e:
            print(f"❌ SMTP connection failed: {e}")
    
    def monitor_email_files(self):
        """Monitor email files in queue directories"""
        print("\n📁 Monitoring Queue Directories:")
        print("-" * 40)
        
        queue_dirs = [
            "/var/spool/postfix/incoming",
            "/var/spool/postfix/active", 
            "/var/spool/postfix/deferred",
            "/var/spool/postfix/hold"
        ]
        
        for queue_dir in queue_dirs:
            if os.path.exists(queue_dir):
                files = os.listdir(queue_dir)
                print(f"{queue_dir}: {len(files)} files")
                if files:
                    for f in files[:5]:  # Show first 5 files
                        print(f"  - {f}")
            else:
                print(f"{queue_dir}: Directory not found")

def main():
    if len(sys.argv) < 2:
        print("📧 Postfix Email Monitor")
        print("=" * 30)
        print("Usage:")
        print("  python3 postfix_monitor.py watch    - Watch logs in real-time")
        print("  python3 postfix_monitor.py queue    - Check mail queue")
        print("  python3 postfix_monitor.py config   - Show Postfix config")
        print("  python3 postfix_monitor.py test     - Test SMTP connection")
        print("  python3 postfix_monitor.py files    - Monitor queue files")
        print("  python3 postfix_monitor.py all      - Show all info")
        return
    
    monitor = PostfixMonitor()
    command = sys.argv[1].lower()
    
    if command == "watch":
        monitor.watch_logs()
    elif command == "queue":
        monitor.check_queue()
    elif command == "config":
        monitor.show_postfix_config()
    elif command == "test":
        monitor.test_smtp_connection()
    elif command == "files":
        monitor.monitor_email_files()
    elif command == "all":
        monitor.show_postfix_config()
        monitor.check_queue()
        monitor.test_smtp_connection()
        monitor.monitor_email_files()
    else:
        print(f"❌ Unknown command: {command}")

if __name__ == "__main__":
    main()
