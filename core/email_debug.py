#!/usr/bin/env python3
"""
Comprehensive Email Debugging Tool
"""

import subprocess
import os
import time
import json
from datetime import datetime

class EmailDebugger:
    def __init__(self):
        self.debug_dir = "/var/log/email_debug"
        os.makedirs(self.debug_dir, exist_ok=True)
    
    def setup_interceptor(self):
        """Set up email interceptor in Postfix"""
        print("🔧 Setting up email interceptor...")
        
        # Create a test alias that uses the interceptor
        alias_line = "debugemail: |/usr/bin/python3 /home/ubuntu/email_interceptor.py"
        
        try:
            # Add debug alias
            with open('/etc/aliases', 'a') as f:
                f.write(f"\n{alias_line}\n")
            
            # Rebuild aliases
            subprocess.run(['newaliases'], check=True)
            
            # Update virtual aliases to include debug
            virtual_aliases = """
# Virtual aliases for reademail.ai - route to interceptor for debugging
@reademail.ai    debugemail@localhost
@mail.reademail.ai    debugemail@localhost
"""
            with open('/etc/postfix/virtual_aliases', 'w') as f:
                f.write(virtual_aliases)
            
            # Rebuild virtual aliases
            subprocess.run(['postmap', '/etc/postfix/virtual_aliases'], check=True)
            
            # Reload Postfix
            subprocess.run(['postfix', 'reload'], check=True)
            
            print("✅ Email interceptor set up successfully")
            print("📧 All emails to @reademail.ai will now be intercepted and logged")
            
        except Exception as e:
            print(f"❌ Error setting up interceptor: {e}")
    
    def watch_intercepted_emails(self):
        """Watch for intercepted emails"""
        print("👀 Watching for intercepted emails...")
        print(f"📁 Debug directory: {self.debug_dir}")
        print("=" * 60)
        
        interceptor_log = f"{self.debug_dir}/interceptor.log"
        
        if not os.path.exists(interceptor_log):
            print("⏳ Waiting for first email to be intercepted...")
        
        try:
            # Watch the interceptor log file
            process = subprocess.Popen(
                ['tail', '-f', interceptor_log],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            for line in process.stdout:
                print(line.strip())
                
        except FileNotFoundError:
            print("⏳ No intercepted emails yet. Send a test email to see activity.")
        except KeyboardInterrupt:
            print("\n🛑 Stopped watching")
            if 'process' in locals():
                process.terminate()
    
    def show_intercepted_emails(self):
        """Show all intercepted emails"""
        print("📧 Intercepted Emails:")
        print("-" * 40)
        
        if not os.path.exists(self.debug_dir):
            print("No intercepted emails found")
            return
        
        files = [f for f in os.listdir(self.debug_dir) if f.startswith('email_info_')]
        files.sort()
        
        if not files:
            print("No intercepted emails found")
            return
        
        for i, file in enumerate(files[-10:], 1):  # Show last 10
            file_path = os.path.join(self.debug_dir, file)
            try:
                with open(file_path, 'r') as f:
                    email_info = json.load(f)
                
                print(f"{i}. {email_info['timestamp']}")
                print(f"   From: {email_info['headers'].get('from', 'Unknown')}")
                print(f"   To: {email_info['headers'].get('to', 'Unknown')}")
                print(f"   Subject: {email_info['headers'].get('subject', 'No Subject')}")
                print(f"   Size: {email_info['size']} bytes")
                print()
                
            except Exception as e:
                print(f"Error reading {file}: {e}")
    
    def show_raw_email(self, email_number=None):
        """Show raw content of intercepted email"""
        files = [f for f in os.listdir(self.debug_dir) if f.startswith('raw_email_')]
        files.sort()
        
        if not files:
            print("No raw emails found")
            return
        
        if email_number is None:
            email_number = len(files)  # Show latest
        
        if email_number > len(files) or email_number < 1:
            print(f"Invalid email number. Available: 1-{len(files)}")
            return
        
        file_path = os.path.join(self.debug_dir, files[email_number - 1])
        
        print(f"📧 Raw Email #{email_number}:")
        print("=" * 60)
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                print(content)
        except Exception as e:
            print(f"Error reading raw email: {e}")
    
    def test_email_flow(self):
        """Send a test email and monitor the flow"""
        print("🧪 Testing email flow...")
        
        test_email = f"""HELO debug.test.com
MAIL FROM: debug@test.com
RCPT TO: debug@reademail.ai
DATA
Subject: Debug Test Email
From: debug@test.com
To: debug@reademail.ai

This is a debug test email sent at {datetime.now()}.
Test ID: DEBUG-{int(time.time())}
.
QUIT"""
        
        try:
            # Send test email
            process = subprocess.Popen(
                ['nc', 'localhost', '25'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(test_email)
            
            print("📤 Test email sent")
            print(f"Response: {stdout}")
            
            if stderr:
                print(f"Errors: {stderr}")
            
            # Wait a moment for processing
            print("⏳ Waiting for email to be processed...")
            time.sleep(5)
            
            # Show results
            self.show_intercepted_emails()
            
        except Exception as e:
            print(f"❌ Error sending test email: {e}")

def main():
    import sys
    
    debugger = EmailDebugger()
    
    if len(sys.argv) < 2:
        print("🐛 Email Debugging Tool")
        print("=" * 30)
        print("Commands:")
        print("  setup     - Set up email interceptor")
        print("  watch     - Watch for intercepted emails")
        print("  list      - Show intercepted emails")
        print("  raw [N]   - Show raw email content (N = email number)")
        print("  test      - Send test email and monitor")
        return
    
    command = sys.argv[1].lower()
    
    if command == "setup":
        debugger.setup_interceptor()
    elif command == "watch":
        debugger.watch_intercepted_emails()
    elif command == "list":
        debugger.show_intercepted_emails()
    elif command == "raw":
        email_num = int(sys.argv[2]) if len(sys.argv) > 2 else None
        debugger.show_raw_email(email_num)
    elif command == "test":
        debugger.test_email_flow()
    else:
        print(f"❌ Unknown command: {command}")

if __name__ == "__main__":
    main()
