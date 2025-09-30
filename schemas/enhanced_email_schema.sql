-- Enhanced Email Processing Schema
-- Adds regex pattern matching, dynamic data extraction, and HTTP integration

USE email_server;

-- Enhanced processing rules table with regex patterns
CREATE TABLE IF NOT EXISTS regex_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    priority INT DEFAULT 100,
    active BOOLEAN DEFAULT TRUE,
    
    -- Regex patterns for matching
    sender_pattern VARCHAR(1000),
    subject_pattern VARCHAR(1000),
    body_pattern VARCHAR(1000),
    
    -- Match conditions (AND/OR logic)
    match_logic ENUM('AND', 'OR') DEFAULT 'AND',
    
    -- Actions to perform
    extract_data BOOLEAN DEFAULT FALSE,
    insert_to_database BOOLEAN DEFAULT FALSE,
    make_http_call BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_priority (priority),
    INDEX idx_active (active)
);

-- Data extraction patterns for capturing specific information
CREATE TABLE IF NOT EXISTS data_extraction_patterns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_id INT NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    source_field ENUM('sender', 'subject', 'body') NOT NULL,
    regex_pattern VARCHAR(1000) NOT NULL,
    capture_group INT DEFAULT 1,
    data_type ENUM('string', 'integer', 'decimal', 'date', 'datetime') DEFAULT 'string',
    required BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (rule_id) REFERENCES regex_rules(id) ON DELETE CASCADE,
    INDEX idx_rule_id (rule_id)
);

-- Database insertion configurations
CREATE TABLE IF NOT EXISTS database_insertions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_id INT NOT NULL,
    target_database VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (rule_id) REFERENCES regex_rules(id) ON DELETE CASCADE,
    INDEX idx_rule_id (rule_id)
);

-- Field mappings for database insertions
CREATE TABLE IF NOT EXISTS database_field_mappings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    insertion_id INT NOT NULL,
    target_field VARCHAR(100) NOT NULL,
    source_type ENUM('extracted_data', 'email_metadata', 'static_value') NOT NULL,
    source_value VARCHAR(500) NOT NULL, -- field name, metadata key, or static value
    data_transformation VARCHAR(500), -- optional transformation function
    
    FOREIGN KEY (insertion_id) REFERENCES database_insertions(id) ON DELETE CASCADE,
    INDEX idx_insertion_id (insertion_id)
);

-- HTTP API call configurations
CREATE TABLE IF NOT EXISTS http_calls (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    method ENUM('GET', 'POST', 'PUT', 'PATCH', 'DELETE') DEFAULT 'POST',
    base_url VARCHAR(1000) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    
    -- Headers and authentication
    headers JSON,
    auth_type ENUM('none', 'basic', 'bearer', 'api_key') DEFAULT 'none',
    auth_config JSON,
    
    -- Retry configuration
    max_retries INT DEFAULT 3,
    retry_delay INT DEFAULT 5, -- seconds
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (rule_id) REFERENCES regex_rules(id) ON DELETE CASCADE,
    INDEX idx_rule_id (rule_id)
);

-- HTTP call parameters (query string and body parameters)
CREATE TABLE IF NOT EXISTS http_call_parameters (
    id INT AUTO_INCREMENT PRIMARY KEY,
    http_call_id INT NOT NULL,
    parameter_name VARCHAR(100) NOT NULL,
    parameter_type ENUM('query', 'body', 'header') DEFAULT 'query',
    source_type ENUM('extracted_data', 'email_metadata', 'static_value') NOT NULL,
    source_value VARCHAR(500) NOT NULL,
    data_transformation VARCHAR(500),
    
    FOREIGN KEY (http_call_id) REFERENCES http_calls(id) ON DELETE CASCADE,
    INDEX idx_http_call_id (http_call_id)
);

-- Execution log for tracking rule processing
CREATE TABLE IF NOT EXISTS rule_executions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    rule_id INT NOT NULL,
    execution_type ENUM('data_extraction', 'database_insertion', 'http_call') NOT NULL,
    status ENUM('success', 'failed', 'skipped') NOT NULL,
    execution_time DECIMAL(10,3), -- milliseconds
    result_data JSON,
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    FOREIGN KEY (rule_id) REFERENCES regex_rules(id) ON DELETE CASCADE,
    INDEX idx_email_id (email_id),
    INDEX idx_rule_id (rule_id),
    INDEX idx_executed_at (executed_at)
);

-- Extracted data storage
CREATE TABLE IF NOT EXISTS extracted_email_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    rule_id INT NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    field_value TEXT,
    data_type VARCHAR(50),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    FOREIGN KEY (rule_id) REFERENCES regex_rules(id) ON DELETE CASCADE,
    INDEX idx_email_id (email_id),
    INDEX idx_rule_id (rule_id),
    UNIQUE KEY unique_email_rule_field (email_id, rule_id, field_name)
);

-- HTTP call logs
CREATE TABLE IF NOT EXISTS http_call_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    http_call_id INT NOT NULL,
    request_url TEXT NOT NULL,
    request_method VARCHAR(10) NOT NULL,
    request_headers JSON,
    request_body TEXT,
    response_status INT,
    response_headers JSON,
    response_body TEXT,
    execution_time DECIMAL(10,3), -- milliseconds
    success BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    FOREIGN KEY (http_call_id) REFERENCES http_calls(id) ON DELETE CASCADE,
    INDEX idx_email_id (email_id),
    INDEX idx_http_call_id (http_call_id),
    INDEX idx_executed_at (executed_at)
);

-- Sample regex rule for demonstration
INSERT INTO regex_rules (name, description, sender_pattern, subject_pattern, body_pattern, extract_data, insert_to_database, make_http_call) 
VALUES (
    'order_notification',
    'Process order notification emails',
    '.*@(shop|store|commerce)\\.(com|net|org)',
    'Order\\s+#?([A-Z0-9-]+)',
    'Total:\\s*\\$([0-9,]+\\.?[0-9]*)',
    TRUE,
    TRUE,
    TRUE
);

-- Sample data extraction patterns
INSERT INTO data_extraction_patterns (rule_id, field_name, source_field, regex_pattern, capture_group, data_type) 
VALUES 
    (1, 'order_number', 'subject', 'Order\\s+#?([A-Z0-9-]+)', 1, 'string'),
    (1, 'total_amount', 'body', 'Total:\\s*\\$([0-9,]+\\.?[0-9]*)', 1, 'decimal'),
    (1, 'customer_email', 'sender', '([^@]+@[^@]+\\.[^@]+)', 1, 'string');

-- Sample database insertion configuration
INSERT INTO database_insertions (rule_id, target_database, target_table) 
VALUES (1, 'email_server', 'order_data');

-- Create the target table for order data
CREATE TABLE IF NOT EXISTS order_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    order_number VARCHAR(100),
    total_amount DECIMAL(10,2),
    customer_email VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    INDEX idx_email_id (email_id),
    INDEX idx_order_number (order_number)
);

-- Sample field mappings for database insertion
INSERT INTO database_field_mappings (insertion_id, target_field, source_type, source_value) 
VALUES 
    (1, 'email_id', 'email_metadata', 'email_id'),
    (1, 'order_number', 'extracted_data', 'order_number'),
    (1, 'total_amount', 'extracted_data', 'total_amount'),
    (1, 'customer_email', 'extracted_data', 'customer_email');

-- Sample HTTP call configuration
INSERT INTO http_calls (rule_id, name, method, base_url, headers) 
VALUES (
    1, 
    'notify_order_webhook', 
    'POST', 
    'https://api.example.com/webhooks/order',
    '{"Content-Type": "application/json", "Authorization": "Bearer YOUR_API_KEY"}'
);

-- Sample HTTP call parameters
INSERT INTO http_call_parameters (http_call_id, parameter_name, parameter_type, source_type, source_value) 
VALUES 
    (1, 'order_id', 'body', 'extracted_data', 'order_number'),
    (1, 'amount', 'body', 'extracted_data', 'total_amount'),
    (1, 'customer', 'body', 'extracted_data', 'customer_email'),
    (1, 'email_id', 'body', 'email_metadata', 'email_id'),
    (1, 'processed_at', 'body', 'email_metadata', 'received_at');

-- Create indexes for performance
CREATE INDEX idx_emails_received_at ON emails(received_at);
CREATE INDEX idx_rule_executions_status ON rule_executions(status);
CREATE INDEX idx_extracted_data_field ON extracted_email_data(field_name);

-- Grant permissions to email processor user
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.regex_rules TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.data_extraction_patterns TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.database_insertions TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.database_field_mappings TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.http_calls TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.http_call_parameters TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.rule_executions TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.extracted_email_data TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.http_call_logs TO 'email_processor'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON email_server.order_data TO 'email_processor'@'localhost';

-- Also grant CREATE and DROP for dynamic table creation
GRANT CREATE, DROP ON email_server.* TO 'email_processor'@'localhost';
