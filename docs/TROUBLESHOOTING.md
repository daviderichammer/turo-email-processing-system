# Troubleshooting Guide

## Common Issues

### Emails Not Being Received
1. Check DNS configuration: `dig MX reademail.ai`
2. Verify Postfix is running: `systemctl status postfix`
3. Check firewall: `ufw status` or `iptables -L`
4. Test SMTP connectivity: `telnet your-server 25`

### Emails Not Being Processed
1. Check Postfix logs: `tail -f /var/log/mail.log`
2. Check processing logs: `tail -f /var/log/email_processor.log`
3. Test processor manually: `echo "test" | python3 /opt/email_processor.py`
4. Verify database connectivity: `mysql -u email_processor -p email_server`

### Categorization Issues
1. Check categorization logs: `tail -f /var/log/turo_categorization.log`
2. Review patterns: `python3 /opt/turo_category_manager.py show 1`
3. Test patterns manually: `python3 /opt/turo_categorization_engine.py --email-id 123`

### Database Issues
1. Check MySQL status: `systemctl status mysql`
2. Verify schema: `mysql -u root -p -e "USE email_server; SHOW TABLES;"`
3. Check permissions: `mysql -u root -p -e "SHOW GRANTS FOR 'email_processor'@'localhost';"`

## Log Locations
- Email Processing: `/var/log/email_processor.log`
- Categorization: `/var/log/turo_categorization.log`
- Postfix: `/var/log/mail.log`
- MySQL: `/var/log/mysql/error.log`

## Performance Tuning
- Increase batch size for large volumes: `--batch 500`
- Adjust confidence thresholds for accuracy vs. coverage
- Optimize regex patterns for better performance
- Use database indexes for frequently queried fields
