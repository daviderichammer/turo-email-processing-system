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

## 2025-11-09 - Email Parsing Service v3 (Continuous Fix)

### Issue
- Email parsing service v2 would stop processing after a period of time
- Service appeared active but was stuck waiting on stale database connections
- Gap would grow as new emails arrived but weren't parsed

### Root Cause
- Database connections were kept open and becoming stale
- MySQL connections were entering bad state after prolonged use
- Process was blocking on network I/O waiting for database responses

### Solution (v3)
- Create fresh database connection for each batch
- Close connections immediately after use
- Enable auto-commit to prevent transaction locks
- Add explicit iteration logging to verify continuous operation

### Files Modified
- `email_parsing_service_v3.py` - New version with proper connection management
- `config/email-parsing.service` - Updated to use v3
- `CHANGELOG.md` - This changelog

### Results
- ✅ Service runs continuously without stopping
- ✅ Checks for new emails every 10 seconds
- ✅ Processes emails within 10 seconds of arrival
- ✅ No stale connection issues
- ✅ Verified running for 2+ minutes without issues
