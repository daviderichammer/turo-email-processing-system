-- Turo Email Dynamic Categorization System
-- This schema supports intelligent email categorization with duplicate detection

USE email_server;

-- Email Categories Table
CREATE TABLE IF NOT EXISTS email_categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    pattern_indicators JSON, -- Store key patterns that identify this category
    confidence_threshold DECIMAL(3,2) DEFAULT 0.80, -- Minimum confidence to auto-assign
    auto_assign BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_category_name (category_name),
    INDEX idx_auto_assign (auto_assign)
);

-- Email Category Assignments
CREATE TABLE IF NOT EXISTS email_category_assignments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    category_id INT NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL,
    assignment_method ENUM('auto', 'manual', 'suggested') NOT NULL,
    assigned_by VARCHAR(50) DEFAULT 'system',
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES email_categories(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_email_category (email_id, category_id),
    INDEX idx_email_id (email_id),
    INDEX idx_category_id (category_id),
    INDEX idx_confidence (confidence_score),
    INDEX idx_method (assignment_method)
);

-- Duplicate Detection and Management
CREATE TABLE IF NOT EXISTS email_duplicates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    primary_email_id INT NOT NULL, -- The email we keep as primary
    duplicate_email_id INT NOT NULL, -- The duplicate email
    similarity_score DECIMAL(3,2) NOT NULL,
    duplicate_type ENUM('exact', 'near_exact', 'content_similar') NOT NULL,
    detection_method VARCHAR(100) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (primary_email_id) REFERENCES emails(id) ON DELETE CASCADE,
    FOREIGN KEY (duplicate_email_id) REFERENCES emails(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_duplicate_pair (primary_email_id, duplicate_email_id),
    INDEX idx_primary_email (primary_email_id),
    INDEX idx_duplicate_email (duplicate_email_id),
    INDEX idx_similarity (similarity_score),
    INDEX idx_duplicate_type (duplicate_type)
);

-- Category Pattern Learning
CREATE TABLE IF NOT EXISTS category_patterns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT NOT NULL,
    pattern_type ENUM('subject', 'sender', 'body', 'combined') NOT NULL,
    pattern_regex VARCHAR(500) NOT NULL,
    pattern_weight DECIMAL(3,2) DEFAULT 1.00,
    success_rate DECIMAL(3,2) DEFAULT 0.00,
    usage_count INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (category_id) REFERENCES email_categories(id) ON DELETE CASCADE,
    
    INDEX idx_category_pattern (category_id, pattern_type),
    INDEX idx_active_patterns (is_active),
    INDEX idx_success_rate (success_rate)
);

-- New Category Suggestions
CREATE TABLE IF NOT EXISTS category_suggestions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    suggested_name VARCHAR(100) NOT NULL,
    description TEXT,
    sample_email_ids JSON, -- Array of email IDs that match this pattern
    pattern_analysis JSON, -- Detected patterns and their confidence
    suggestion_confidence DECIMAL(3,2) NOT NULL,
    status ENUM('pending', 'approved', 'rejected', 'merged') DEFAULT 'pending',
    reviewed_by VARCHAR(50) NULL,
    reviewed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_status (status),
    INDEX idx_confidence (suggestion_confidence),
    INDEX idx_created (created_at)
);

-- Turo-Specific Data Extraction Tables
CREATE TABLE IF NOT EXISTS turo_messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    guest_name VARCHAR(100),
    vehicle_info VARCHAR(200),
    reservation_id VARCHAR(50),
    message_content TEXT,
    message_url VARCHAR(500),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_email_message (email_id),
    INDEX idx_guest_name (guest_name),
    INDEX idx_reservation_id (reservation_id),
    INDEX idx_vehicle_info (vehicle_info)
);

CREATE TABLE IF NOT EXISTS turo_bookings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    reservation_id VARCHAR(50),
    guest_name VARCHAR(100),
    vehicle_info VARCHAR(200),
    booking_status ENUM('requested', 'confirmed', 'cancelled', 'completed') NOT NULL,
    start_date DATE,
    end_date DATE,
    total_amount DECIMAL(10,2),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_email_booking (email_id),
    INDEX idx_reservation_id (reservation_id),
    INDEX idx_guest_name (guest_name),
    INDEX idx_booking_status (booking_status),
    INDEX idx_dates (start_date, end_date)
);

CREATE TABLE IF NOT EXISTS turo_payments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    reservation_id VARCHAR(50),
    payment_type ENUM('payout', 'charge', 'refund', 'fee') NOT NULL,
    amount DECIMAL(10,2),
    payment_date DATE,
    description TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_email_payment (email_id),
    INDEX idx_reservation_id (reservation_id),
    INDEX idx_payment_type (payment_type),
    INDEX idx_amount (amount),
    INDEX idx_payment_date (payment_date)
);

-- Processing Status Tracking
CREATE TABLE IF NOT EXISTS email_processing_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email_id INT NOT NULL,
    categorization_status ENUM('pending', 'categorized', 'suggested', 'failed') DEFAULT 'pending',
    duplicate_check_status ENUM('pending', 'checked', 'is_duplicate', 'failed') DEFAULT 'pending',
    extraction_status ENUM('pending', 'extracted', 'no_data', 'failed') DEFAULT 'pending',
    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    processing_notes TEXT,
    
    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_email_status (email_id),
    INDEX idx_categorization_status (categorization_status),
    INDEX idx_duplicate_status (duplicate_check_status),
    INDEX idx_extraction_status (extraction_status)
);

-- Insert Initial Turo Categories Based on Analysis
INSERT INTO email_categories (category_name, description, pattern_indicators, confidence_threshold) VALUES
('guest_message', 'Messages from guests about their reservations', 
 '{"subject_patterns": ["has sent you a message about your"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.90),

('booking_request', 'New booking requests from guests', 
 '{"subject_patterns": ["booking request", "wants to book"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.85),

('booking_confirmation', 'Confirmed bookings and reservations', 
 '{"subject_patterns": ["confirmed", "booking confirmed"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.85),

('cancellation', 'Booking cancellations and related notifications', 
 '{"subject_patterns": ["cancelled", "cancellation"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.85),

('payment_notification', 'Payment, payout, and financial notifications', 
 '{"subject_patterns": ["payment", "payout", "earnings"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.80),

('review_request', 'Review requests and feedback notifications', 
 '{"subject_patterns": ["review", "rate your experience"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.80),

('reminder', 'Various reminders and follow-up notifications', 
 '{"subject_patterns": ["reminder", "don''t forget"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.75),

('system_notification', 'General system notifications and updates', 
 '{"subject_patterns": ["notification", "update", "alert"], "sender_patterns": ["noreply@mail.turo.com"]}', 0.70);

-- Insert Pattern Rules for Guest Messages
INSERT INTO category_patterns (category_id, pattern_type, pattern_regex, pattern_weight) VALUES
(1, 'subject', '.*has sent you a message about your.*', 1.00),
(1, 'sender', 'noreply@mail\.turo\.com', 0.80),
(1, 'body', '.*Reply https://turo\.com/us/en/reservation/.*', 0.60);

-- Insert Pattern Rules for Booking Confirmations  
INSERT INTO category_patterns (category_id, pattern_type, pattern_regex, pattern_weight) VALUES
(3, 'subject', '.*(confirmed|booking confirmed).*', 1.00),
(3, 'sender', 'noreply@mail\.turo\.com', 0.80);

-- Insert Pattern Rules for Cancellations
INSERT INTO category_patterns (category_id, pattern_type, pattern_regex, pattern_weight) VALUES
(4, 'subject', '.*(cancelled|cancellation).*', 1.00),
(4, 'sender', 'noreply@mail\.turo\.com', 0.80);

-- Add duplicate detection status to existing emails table if not exists
ALTER TABLE emails 
ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS duplicate_of_email_id INT NULL,
ADD INDEX IF NOT EXISTS idx_is_duplicate (is_duplicate),
ADD INDEX IF NOT EXISTS idx_duplicate_of (duplicate_of_email_id);
