# Changelog

## 2025-11-04 - Email Parsing Fix

### Issue
- Email parsing stopped on October 2, 2025 at 13:20
- Last parsed email was #1609
- 26,339 emails remained unprocessed

### Root Cause
- No automated background service running to parse emails
- Original parsing script had syntax errors

### Solution
- Created standalone parsing service (v2)
- Processes emails independently of ingestion
- Saves only best template match per email (1 entry per email)
- Implemented as systemd service for reliability

### Files Added/Modified
- `scripts/email_parsing_service_v2.py` - New standalone parser
- `config/email-parsing.service` - Systemd service configuration
- System organized in `/root/email-system/`

### Results
- ✅ All 27,955+ emails parsed
- ✅ 1 entry per email (no duplicates)
- ✅ Continuous parsing service running
- ✅ Email ingestion unchanged and working
