#!/usr/bin/python3
"""
HTTP API Integration Module for Enhanced Email Processor
Handles HTTP API calls based on configurable rules and extracted data
"""

import logging
import requests
import json
import time
import mysql.connector
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urlencode

class HTTPAPIEngine:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        
    def get_http_calls(self, rule_id: int) -> List[Dict[str, Any]]:
        """Get HTTP call configurations for a rule"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM http_calls 
                WHERE rule_id = %s AND active = TRUE
                ORDER BY id ASC
            """
            
            cursor.execute(query, (rule_id,))
            http_calls = cursor.fetchall()
            
            logging.info(f"Retrieved {len(http_calls)} HTTP calls for rule {rule_id}")
            return http_calls
            
        except Exception as e:
            logging.error(f"Failed to retrieve HTTP calls: {e}")
            return []
        finally:
            cursor.close()
    
    def get_http_parameters(self, http_call_id: int) -> List[Dict[str, Any]]:
        """Get HTTP call parameters for a specific HTTP call"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM http_call_parameters 
                WHERE http_call_id = %s 
                ORDER BY id ASC
            """
            
            cursor.execute(query, (http_call_id,))
            parameters = cursor.fetchall()
            
            return parameters
            
        except Exception as e:
            logging.error(f"Failed to retrieve HTTP parameters: {e}")
            return []
        finally:
            cursor.close()
    
    def resolve_parameter_value(self, parameter: Dict[str, Any], email_data: Dict[str, Any], 
                               extracted_data: Dict[str, Any], email_id: int) -> Any:
        """Resolve the value for an HTTP parameter based on its source type"""
        try:
            source_type = parameter['source_type']
            source_value = parameter['source_value']
            transformation = parameter['data_transformation']
            
            # Get the raw value based on source type
            if source_type == 'extracted_data':
                raw_value = extracted_data.get(source_value)
            elif source_type == 'email_metadata':
                raw_value = self.get_email_metadata_value(source_value, email_data, email_id)
            elif source_type == 'static_value':
                raw_value = source_value
            else:
                logging.warning(f"Unknown source type: {source_type}")
                return None
            
            # Apply transformation if specified
            if transformation and raw_value is not None:
                raw_value = self.apply_transformation(raw_value, transformation)
            
            return raw_value
            
        except Exception as e:
            logging.error(f"Failed to resolve parameter value: {e}")
            return None
    
    def get_email_metadata_value(self, metadata_key: str, email_data: Dict[str, Any], email_id: int) -> Any:
        """Get email metadata value"""
        metadata_map = {
            'email_id': email_id,
            'sender': email_data.get('sender', ''),
            'recipient': email_data.get('recipient', ''),
            'subject': email_data.get('subject', ''),
            'message_id': email_data.get('message_id', ''),
            'received_at': datetime.now().isoformat(),
            'processed_at': datetime.now().isoformat(),
            'timestamp': int(time.time())
        }
        
        return metadata_map.get(metadata_key)
    
    def apply_transformation(self, value: Any, transformation: str) -> Any:
        """Apply data transformation to a value"""
        try:
            # Basic transformations
            if transformation == 'upper':
                return str(value).upper()
            elif transformation == 'lower':
                return str(value).lower()
            elif transformation == 'strip':
                return str(value).strip()
            elif transformation == 'int':
                return int(float(str(value)))
            elif transformation == 'float':
                return float(str(value))
            elif transformation == 'url_encode':
                from urllib.parse import quote
                return quote(str(value))
            elif transformation == 'json_encode':
                return json.dumps(value)
            elif transformation.startswith('substring:'):
                # Format: substring:start:end
                parts = transformation.split(':')
                if len(parts) >= 2:
                    start = int(parts[1]) if parts[1] else 0
                    end = int(parts[2]) if len(parts) > 2 and parts[2] else None
                    return str(value)[start:end]
            elif transformation.startswith('replace:'):
                # Format: replace:old:new
                parts = transformation.split(':', 2)
                if len(parts) == 3:
                    return str(value).replace(parts[1], parts[2])
            elif transformation.startswith('regex:'):
                # Format: regex:pattern:replacement
                import re
                parts = transformation.split(':', 2)
                if len(parts) == 3:
                    return re.sub(parts[1], parts[2], str(value))
            elif transformation.startswith('format:'):
                # Format: format:template (e.g., format:Order {0} processed)
                template = transformation[7:]  # Remove 'format:' prefix
                return template.format(value)
            
            # If no transformation matches, return original value
            logging.warning(f"Unknown transformation: {transformation}")
            return value
            
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            return value
    
    def build_request_url(self, http_call: Dict[str, Any], query_params: Dict[str, Any]) -> str:
        """Build the complete request URL with query parameters"""
        base_url = http_call['base_url'].rstrip('/')
        
        if query_params:
            # Filter out None values
            filtered_params = {k: v for k, v in query_params.items() if v is not None}
            if filtered_params:
                query_string = urlencode(filtered_params)
                return f"{base_url}?{query_string}"
        
        return base_url
    
    def build_request_headers(self, http_call: Dict[str, Any], header_params: Dict[str, Any]) -> Dict[str, str]:
        """Build request headers"""
        headers = {}
        
        # Add configured headers
        if http_call['headers']:
            try:
                configured_headers = json.loads(http_call['headers'])
                headers.update(configured_headers)
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON in headers for HTTP call {http_call['id']}")
        
        # Add dynamic header parameters
        for key, value in header_params.items():
            if value is not None:
                headers[key] = str(value)
        
        # Add authentication headers
        auth_type = http_call['auth_type']
        if auth_type != 'none' and http_call['auth_config']:
            try:
                auth_config = json.loads(http_call['auth_config'])
                
                if auth_type == 'bearer':
                    token = auth_config.get('token')
                    if token:
                        headers['Authorization'] = f"Bearer {token}"
                
                elif auth_type == 'api_key':
                    api_key = auth_config.get('api_key')
                    header_name = auth_config.get('header_name', 'X-API-Key')
                    if api_key:
                        headers[header_name] = api_key
                
                elif auth_type == 'basic':
                    import base64
                    username = auth_config.get('username')
                    password = auth_config.get('password')
                    if username and password:
                        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                        headers['Authorization'] = f"Basic {credentials}"
                        
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON in auth_config for HTTP call {http_call['id']}")
        
        return headers
    
    def build_request_body(self, http_call: Dict[str, Any], body_params: Dict[str, Any]) -> Optional[str]:
        """Build request body"""
        if not body_params:
            return None
        
        # Filter out None values
        filtered_params = {k: v for k, v in body_params.items() if v is not None}
        
        if not filtered_params:
            return None
        
        # Default to JSON format
        try:
            return json.dumps(filtered_params)
        except Exception as e:
            logging.error(f"Failed to build request body: {e}")
            return None
    
    def make_http_request(self, http_call: Dict[str, Any], email_data: Dict[str, Any], 
                         extracted_data: Dict[str, Any], email_id: int) -> Dict[str, Any]:
        """Make an HTTP request based on configuration"""
        request_start_time = time.time()
        result = {
            'success': False,
            'status_code': None,
            'response_body': None,
            'error_message': None,
            'execution_time': 0,
            'request_url': None,
            'request_headers': None,
            'request_body': None
        }
        
        try:
            # Get parameters for this HTTP call
            parameters = self.get_http_parameters(http_call['id'])
            
            # Organize parameters by type
            query_params = {}
            header_params = {}
            body_params = {}
            
            for param in parameters:
                param_name = param['parameter_name']
                param_type = param['parameter_type']
                param_value = self.resolve_parameter_value(param, email_data, extracted_data, email_id)
                
                if param_type == 'query':
                    query_params[param_name] = param_value
                elif param_type == 'header':
                    header_params[param_name] = param_value
                elif param_type == 'body':
                    body_params[param_name] = param_value
            
            # Build request components
            request_url = self.build_request_url(http_call, query_params)
            request_headers = self.build_request_headers(http_call, header_params)
            request_body = self.build_request_body(http_call, body_params)
            
            # Set content type for JSON body
            if request_body and 'Content-Type' not in request_headers:
                request_headers['Content-Type'] = 'application/json'
            
            # Store request details
            result['request_url'] = request_url
            result['request_headers'] = request_headers
            result['request_body'] = request_body
            
            # Make the HTTP request
            method = http_call['method'].upper()
            timeout = 30  # Default timeout
            
            logging.info(f"Making {method} request to {request_url}")
            
            response = requests.request(
                method=method,
                url=request_url,
                headers=request_headers,
                data=request_body,
                timeout=timeout
            )
            
            # Process response
            result['status_code'] = response.status_code
            result['response_body'] = response.text
            result['success'] = 200 <= response.status_code < 300
            
            if result['success']:
                logging.info(f"HTTP request successful: {response.status_code}")
            else:
                logging.warning(f"HTTP request failed: {response.status_code} - {response.text}")
                result['error_message'] = f"HTTP {response.status_code}: {response.text}"
            
        except requests.exceptions.Timeout:
            result['error_message'] = "Request timeout"
            logging.error("HTTP request timeout")
            
        except requests.exceptions.ConnectionError:
            result['error_message'] = "Connection error"
            logging.error("HTTP request connection error")
            
        except Exception as e:
            result['error_message'] = str(e)
            logging.error(f"HTTP request failed: {e}")
        
        finally:
            result['execution_time'] = (time.time() - request_start_time) * 1000
        
        return result
    
    def log_http_call(self, email_id: int, http_call_id: int, request_result: Dict[str, Any]):
        """Log HTTP call results to database"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                INSERT INTO http_call_logs 
                (email_id, http_call_id, request_url, request_method, request_headers, 
                 request_body, response_status, response_body, execution_time, 
                 success, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Convert headers to JSON
            headers_json = json.dumps(request_result['request_headers']) if request_result['request_headers'] else None
            
            cursor.execute(query, (
                email_id,
                http_call_id,
                request_result['request_url'],
                'POST',  # Default method, could be extracted from http_call
                headers_json,
                request_result['request_body'],
                request_result['status_code'],
                request_result['response_body'],
                request_result['execution_time'],
                request_result['success'],
                request_result['error_message']
            ))
            
            self.db_connection.commit()
            logging.info(f"HTTP call logged for email {email_id}")
            
        except Exception as e:
            logging.error(f"Failed to log HTTP call: {e}")
        finally:
            cursor.close()
    
    def process_http_calls(self, rule_id: int, email_data: Dict[str, Any], 
                          extracted_data: Dict[str, Any], email_id: int) -> Dict[str, Any]:
        """Process all HTTP calls for a rule"""
        results = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'call_details': []
        }
        
        try:
            # Get HTTP call configurations
            http_calls = self.get_http_calls(rule_id)
            results['total_calls'] = len(http_calls)
            
            for http_call in http_calls:
                call_start_time = time.time()
                
                try:
                    # Make HTTP request with retry logic
                    max_retries = http_call.get('max_retries', 3)
                    retry_delay = http_call.get('retry_delay', 5)
                    
                    request_result = None
                    
                    for attempt in range(max_retries + 1):
                        request_result = self.make_http_request(http_call, email_data, extracted_data, email_id)
                        
                        if request_result['success']:
                            break
                        
                        if attempt < max_retries:
                            logging.info(f"HTTP call failed, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                    
                    # Log the HTTP call
                    self.log_http_call(email_id, http_call['id'], request_result)
                    
                    call_time = (time.time() - call_start_time) * 1000
                    
                    if request_result['success']:
                        results['successful_calls'] += 1
                        status = 'success'
                        error_msg = None
                    else:
                        results['failed_calls'] += 1
                        status = 'failed'
                        error_msg = request_result['error_message']
                    
                    results['call_details'].append({
                        'http_call_id': http_call['id'],
                        'name': http_call['name'],
                        'method': http_call['method'],
                        'url': request_result['request_url'],
                        'status': status,
                        'status_code': request_result['status_code'],
                        'execution_time': call_time,
                        'error_message': error_msg
                    })
                    
                except Exception as e:
                    results['failed_calls'] += 1
                    call_time = (time.time() - call_start_time) * 1000
                    
                    results['call_details'].append({
                        'http_call_id': http_call['id'],
                        'name': http_call['name'],
                        'method': http_call['method'],
                        'url': http_call['base_url'],
                        'status': 'failed',
                        'status_code': None,
                        'execution_time': call_time,
                        'error_message': str(e)
                    })
                    
                    logging.error(f"HTTP call failed for call {http_call['id']}: {e}")
            
            logging.info(f"HTTP calls completed: {results['successful_calls']}/{results['total_calls']} successful")
            return results
            
        except Exception as e:
            logging.error(f"HTTP call processing failed: {e}")
            results['failed_calls'] = results['total_calls']
            return results
