# Installation Guide

## System Requirements
- Ubuntu 22.04+ server
- MySQL 8.0+
- Python 3.10+
- Postfix mail server
- 4GB+ RAM recommended
- 50GB+ disk space

## Step-by-Step Installation

### 1. Server Preparation
```bash
# Update system
apt update && apt upgrade -y

# Install required packages
apt install -y postfix mysql-server python3 python3-pip git
```

### 2. MySQL Setup
```bash
# Secure MySQL installation
mysql_secure_installation

# Create database and user
mysql -u root -p << 'SQL'
CREATE DATABASE email_server;
CREATE USER 'email_processor'@'localhost' IDENTIFIED BY 'your_secure_password';
GRANT ALL PRIVILEGES ON email_server.* TO 'email_processor'@'localhost';
FLUSH PRIVILEGES;
SQL
```

### 3. Email System Installation
```bash
# Clone repository
git clone https://github.com/yourusername/turo-email-system.git
cd turo-email-system

# Install database schemas
mysql -u root -p email_server < schemas/enhanced_email_schema.sql
mysql -u root -p email_server < schemas/turo_categorization_schema.sql

# Install Python dependencies
pip3 install mysql-connector-python requests tabulate

# Deploy system files
cp core/*.py /opt/
cp processors/*.py /opt/
cp categorization/*.py /opt/
chmod +x /opt/*.py
```

### 4. Postfix Configuration
```bash
# Configure Postfix for receive-only operation
cp config/main.cf /etc/postfix/
cp config/master.cf /etc/postfix/
cp config/aliases /etc/aliases

# Update aliases and restart
newaliases
systemctl restart postfix
systemctl enable postfix
```

### 5. DNS Configuration
Configure your domain's MX record to point to your server:
```
reademail.ai.    MX    0    mail.reademail.ai.
mail.reademail.ai.    A    YOUR_SERVER_IP
```

### 6. Testing
```bash
# Test email processing
echo "Test email" | mail -s "Test" admin@reademail.ai

# Check processing
python3 /opt/email_server_admin.py list
```
