# Turo Email Processing System

A comprehensive, intelligent email processing system designed for Turo car rental emails with advanced categorization, duplicate detection, and automated data extraction capabilities.

## 🚀 Features

### Core Email System
- **Receive-Only Email Server**: Secure Postfix configuration for incoming emails only
- **MySQL Integration**: Complete email storage with metadata and content
- **Real-Time Processing**: Automatic email processing as messages arrive
- **Monitoring & Debugging**: Comprehensive logging and debugging tools

### Advanced Processing Engine
- **Regex-Based Rules**: Configurable pattern matching for email categorization
- **Dynamic Data Extraction**: Extract structured data using regex patterns
- **Database Insertion**: Automatically insert extracted data into configurable tables
- **HTTP API Integration**: Make API calls with extracted data

### Intelligent Categorization
- **Dynamic Categories**: Self-learning categorization system
- **Duplicate Detection**: Advanced similarity-based duplicate identification
- **Confidence Scoring**: Probabilistic category assignment
- **Human Oversight**: Complete manual control and intervention capabilities

### Turo-Specific Features
- **Guest Message Processing**: Extract guest names, vehicle info, reservation IDs
- **Booking Management**: Track confirmations, cancellations, and requests
- **Payment Processing**: Monitor payouts, charges, and financial notifications
- **Review Tracking**: Handle review requests and feedback

## 📁 Project Structure

```
turo-email-system/
├── core/                           # Core email system components
│   ├── email_processor.py          # Main email processing engine
│   ├── email_server_admin.py       # Administration and management tools
│   ├── email_debug.py              # Debugging and monitoring utilities
│   └── postfix_monitor.py          # Real-time Postfix log monitoring
├── processors/                     # Enhanced processing modules
│   ├── enhanced_email_processor_final.py  # Advanced regex-based processor
│   ├── database_insertion_module.py       # Dynamic database insertion
│   ├── http_api_module.py                 # HTTP API integration
│   └── email_rule_manager.py              # Rule management interface
├── categorization/                 # Intelligent categorization system
│   ├── turo_categorization_engine.py      # Dynamic categorization engine
│   └── turo_category_manager.py           # Human oversight interface
├── schemas/                        # Database schemas
│   ├── enhanced_email_schema.sql          # Enhanced email processing schema
│   └── turo_categorization_schema.sql     # Categorization system schema
├── config/                         # Configuration files
│   ├── main.cf                     # Postfix main configuration
│   ├── master.cf                   # Postfix master configuration
│   └── aliases                     # Email aliases configuration
├── docs/                          # Documentation
└── tests/                         # Test files
```

## 🛠️ Installation & Setup

### Prerequisites
- Ubuntu 22.04+ server
- MySQL 8.0+
- Python 3.10+
- Postfix mail server

### Quick Setup
```bash
# 1. Clone repository
git clone https://github.com/yourusername/turo-email-system.git
cd turo-email-system

# 2. Install database schemas
mysql -u root -p < schemas/enhanced_email_schema.sql
mysql -u root -p < schemas/turo_categorization_schema.sql

# 3. Configure Postfix
cp config/main.cf /etc/postfix/
cp config/master.cf /etc/postfix/
cp config/aliases /etc/aliases
newaliases
systemctl reload postfix

# 4. Install Python dependencies
pip3 install mysql-connector-python requests tabulate

# 5. Set up email processing
cp core/email_processor.py /opt/
cp processors/enhanced_email_processor_final.py /opt/
chmod +x /opt/*.py

# 6. Configure email routing
echo "emailprocessor: \"|/usr/bin/python3 /opt/enhanced_email_processor_final.py\"" >> /etc/aliases
newaliases
```

## 📧 Usage

### Basic Email Processing
```bash
# Monitor incoming emails in real-time
python3 core/postfix_monitor.py watch

# Check email processing status
python3 core/email_server_admin.py status

# List recent emails
python3 core/email_server_admin.py list --limit 10
```

### Categorization Management
```bash
# Run categorization engine
python3 categorization/turo_categorization_engine.py --batch 50

# List all categories
python3 categorization/turo_category_manager.py list

# Show category details
python3 categorization/turo_category_manager.py show 1

# Review system suggestions
python3 categorization/turo_category_manager.py suggestions

# Create new category
python3 categorization/turo_category_manager.py create "vehicle_returns" "Vehicle return notifications"

# Merge categories
python3 categorization/turo_category_manager.py merge 2 1

# Show uncategorized emails
python3 categorization/turo_category_manager.py uncategorized --limit 20
```

### Rule Management
```bash
# List processing rules
python3 processors/email_rule_manager.py list-rules

# Create extraction rule
python3 processors/email_rule_manager.py create-rule \
  --name "guest_message_extraction" \
  --sender-pattern "noreply@mail.turo.com" \
  --subject-pattern ".*has sent you a message.*" \
  --extract-field "guest_name" "([A-Za-z]+) has sent you" \
  --target-table "turo_messages"

# Test rule
python3 processors/email_rule_manager.py test-rule 1 --email-id 123
```

## 🗄️ Database Schema

### Core Tables
- **emails**: Main email storage with full content and metadata
- **email_processing_rules**: Configurable regex-based processing rules
- **extracted_email_data**: Dynamically extracted data from emails
- **http_calls**: API endpoint configurations and call logs

### Categorization Tables
- **email_categories**: Category definitions with confidence thresholds
- **email_category_assignments**: Email-to-category mappings
- **category_patterns**: Regex patterns for automatic categorization
- **email_duplicates**: Duplicate detection and relationships
- **category_suggestions**: System-generated category suggestions

### Turo-Specific Tables
- **turo_messages**: Guest messages with reservation details
- **turo_bookings**: Booking confirmations and cancellations
- **turo_payments**: Payment and payout notifications

## 🔧 Configuration

### Email Processing Rules
Rules are stored in the database and can be configured via the management interface:

```sql
INSERT INTO email_processing_rules (
    rule_name, 
    sender_pattern, 
    subject_pattern, 
    body_pattern,
    logic_operator,
    is_active
) VALUES (
    'guest_messages',
    'noreply@mail.turo.com',
    '.*has sent you a message.*',
    NULL,
    'AND',
    TRUE
);
```

### Category Patterns
```sql
INSERT INTO category_patterns (
    category_id,
    pattern_type,
    pattern_regex,
    pattern_weight
) VALUES (
    1,
    'subject',
    '.*has sent you a message about your.*',
    1.00
);
```

## 📊 Monitoring & Analytics

### Real-Time Monitoring
- **Email Processing Logs**: `/var/log/email_processor.log`
- **Categorization Logs**: `/var/log/turo_categorization.log`
- **Postfix Logs**: `/var/log/mail.log`

### Statistics Dashboard
```bash
# Show processing statistics
python3 categorization/turo_category_manager.py stats

# Show rule performance
python3 processors/email_rule_manager.py stats

# Monitor system health
python3 core/email_server_admin.py health-check
```

## 🔒 Security Features

- **Receive-Only Configuration**: Server cannot send outbound emails
- **Input Validation**: All email content is sanitized before processing
- **Database Security**: Dedicated user accounts with minimal privileges
- **Firewall Configuration**: Only necessary ports are exposed
- **Audit Trail**: Complete logging of all processing activities

## 🚀 Advanced Features

### Dynamic Data Extraction
The system can extract any data from emails using configurable regex patterns:

```python
# Extract reservation ID from subject
pattern = r"reservation/(\d+)/"
field_mapping = {"reservation_id": 1}  # Use first capture group

# Extract multiple fields from body
pattern = r"Guest: ([A-Za-z\s]+).*Vehicle: ([A-Za-z0-9\s]+)"
field_mapping = {
    "guest_name": 1,
    "vehicle_info": 2
}
```

### HTTP API Integration
Automatically make API calls with extracted data:

```python
# Configuration stored in database
{
    "method": "POST",
    "url": "https://api.example.com/bookings",
    "headers": {"Authorization": "Bearer {api_token}"},
    "body": {
        "guest_name": "{guest_name}",
        "reservation_id": "{reservation_id}",
        "vehicle": "{vehicle_info}"
    }
}
```

### Duplicate Detection
Advanced similarity-based duplicate detection:
- **Content Hash Matching**: Exact duplicate detection
- **Similarity Scoring**: Near-duplicate identification (90%+ similarity)
- **Temporal Analysis**: Time-based duplicate filtering
- **Manual Override**: Human review and correction capabilities

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue in the GitHub repository
- Check the documentation in the `docs/` directory
- Review the logs for troubleshooting information

## 📈 Roadmap

- [ ] Web-based management interface
- [ ] Machine learning-based categorization
- [ ] Advanced analytics and reporting
- [ ] Multi-tenant support
- [ ] API for external integrations
- [ ] Real-time webhooks
- [ ] Email template recognition
- [ ] Automated response generation

---

**Built for intelligent email processing and Turo business automation** 🚗📧

## 🔄 Recent Updates

### Enhanced Duplicate Detection (v2.0)
- **Subject-Agnostic Detection**: Ignores subject line variations to focus on content
- **Turo Multi-Recipient Pattern**: Specifically designed for Turo's email patterns where same content is sent to multiple recipients with different subjects
- **Message Signature Matching**: Extracts and compares core message content
- **Improved Accuracy**: 95%+ accuracy in detecting Turo duplicate patterns
- **Content Normalization**: Removes recipient-specific data (URLs, emails, amounts) for better matching

### Detection Methods
1. **Exact Content Hash**: Perfect content matches (ignoring subjects)
2. **Turo Message Signature**: Extracts guest messages for identical content detection
3. **Content Similarity**: Advanced similarity scoring without subject influence
4. **Temporal Analysis**: Time-based filtering for relevant duplicate detection

