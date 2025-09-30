# API Documentation

## Email Processing Rules API

### Create Processing Rule
```python
from processors.email_rule_manager import EmailRuleManager

manager = EmailRuleManager()
rule_id = manager.create_rule(
    name="guest_messages",
    sender_pattern="noreply@mail.turo.com",
    subject_pattern=".*has sent you a message.*",
    extract_fields={
        "guest_name": r"([A-Za-z]+) has sent you",
        "vehicle_info": r"about your ([A-Za-z0-9\s]+)"
    },
    target_table="turo_messages"
)
```

### HTTP Integration
```python
# Configure HTTP call
http_config = {
    "method": "POST",
    "url": "https://api.example.com/webhooks/turo",
    "headers": {
        "Authorization": "Bearer {api_token}",
        "Content-Type": "application/json"
    },
    "body": {
        "event": "guest_message",
        "guest_name": "{guest_name}",
        "vehicle": "{vehicle_info}",
        "timestamp": "{received_date}"
    }
}
```

## Categorization API

### Manual Category Assignment
```python
from categorization.turo_category_manager import TuroCategoryManager

manager = TuroCategoryManager()
manager.reassign_emails([123, 124, 125], category_id=2)
```

### Batch Processing
```python
from categorization.turo_categorization_engine import TuroCategorizer

categorizer = TuroCategorizer()
results = categorizer.process_batch(limit=100)
```
