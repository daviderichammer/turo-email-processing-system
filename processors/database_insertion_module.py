#!/usr/bin/python3
"""
Database Insertion Module for Enhanced Email Processor
Handles dynamic database insertion based on configurable rules
"""

import logging
import mysql.connector
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

class DatabaseInsertionEngine:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        
    def get_database_insertions(self, rule_id: int) -> List[Dict[str, Any]]:
        """Get database insertion configurations for a rule"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM database_insertions 
                WHERE rule_id = %s AND active = TRUE
                ORDER BY id ASC
            """
            
            cursor.execute(query, (rule_id,))
            insertions = cursor.fetchall()
            
            logging.info(f"Retrieved {len(insertions)} database insertions for rule {rule_id}")
            return insertions
            
        except Exception as e:
            logging.error(f"Failed to retrieve database insertions: {e}")
            return []
        finally:
            cursor.close()
    
    def get_field_mappings(self, insertion_id: int) -> List[Dict[str, Any]]:
        """Get field mappings for a database insertion"""
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM database_field_mappings 
                WHERE insertion_id = %s 
                ORDER BY id ASC
            """
            
            cursor.execute(query, (insertion_id,))
            mappings = cursor.fetchall()
            
            return mappings
            
        except Exception as e:
            logging.error(f"Failed to retrieve field mappings: {e}")
            return []
        finally:
            cursor.close()
    
    def resolve_field_value(self, mapping: Dict[str, Any], email_data: Dict[str, Any], 
                           extracted_data: Dict[str, Any], email_id: int) -> Any:
        """Resolve the value for a field based on its source type"""
        try:
            source_type = mapping['source_type']
            source_value = mapping['source_value']
            transformation = mapping['data_transformation']
            
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
            logging.error(f"Failed to resolve field value: {e}")
            return None
    
    def get_email_metadata_value(self, metadata_key: str, email_data: Dict[str, Any], email_id: int) -> Any:
        """Get email metadata value"""
        metadata_map = {
            'email_id': email_id,
            'sender': email_data.get('sender', ''),
            'recipient': email_data.get('recipient', ''),
            'subject': email_data.get('subject', ''),
            'message_id': email_data.get('message_id', ''),
            'received_at': datetime.now(),
            'processed_at': datetime.now()
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
            
            # If no transformation matches, return original value
            logging.warning(f"Unknown transformation: {transformation}")
            return value
            
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            return value
    
    def ensure_table_exists(self, database_name: str, table_name: str, field_mappings: List[Dict[str, Any]]) -> bool:
        """Ensure target table exists, create if necessary"""
        try:
            cursor = self.db_connection.cursor()
            
            # Check if table exists
            cursor.execute(f"USE {database_name}")
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            
            if cursor.fetchone():
                logging.debug(f"Table {database_name}.{table_name} already exists")
                return True
            
            # Create table dynamically
            logging.info(f"Creating table {database_name}.{table_name}")
            
            # Build CREATE TABLE statement
            columns = []
            columns.append("id INT AUTO_INCREMENT PRIMARY KEY")
            
            for mapping in field_mappings:
                field_name = mapping['target_field']
                
                # Skip id field as it's already added
                if field_name.lower() == 'id':
                    continue
                
                # Determine column type based on source
                if mapping['source_type'] == 'email_metadata' and mapping['source_value'] == 'email_id':
                    column_def = f"{field_name} INT"
                elif 'date' in field_name.lower() or 'time' in field_name.lower():
                    column_def = f"{field_name} TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                elif 'amount' in field_name.lower() or 'price' in field_name.lower() or 'total' in field_name.lower():
                    column_def = f"{field_name} DECIMAL(10,2)"
                elif 'email' in field_name.lower():
                    column_def = f"{field_name} VARCHAR(255)"
                elif 'number' in field_name.lower() or 'id' in field_name.lower():
                    column_def = f"{field_name} VARCHAR(100)"
                else:
                    column_def = f"{field_name} TEXT"
                
                columns.append(column_def)
            
            # Add timestamp
            columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            # Add foreign key for email_id if present
            for mapping in field_mappings:
                if (mapping['source_type'] == 'email_metadata' and 
                    mapping['source_value'] == 'email_id' and 
                    mapping['target_field'] != 'id'):
                    
                    try:
                        fk_sql = f"""
                            ALTER TABLE {table_name} 
                            ADD FOREIGN KEY ({mapping['target_field']}) 
                            REFERENCES emails(id) ON DELETE CASCADE
                        """
                        cursor.execute(fk_sql)
                        logging.info(f"Added foreign key constraint for {mapping['target_field']}")
                    except Exception as e:
                        logging.warning(f"Failed to add foreign key: {e}")
            
            self.db_connection.commit()
            logging.info(f"Successfully created table {database_name}.{table_name}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to ensure table exists: {e}")
            return False
        finally:
            cursor.close()
    
    def insert_data_to_database(self, insertion_config: Dict[str, Any], email_data: Dict[str, Any], 
                               extracted_data: Dict[str, Any], email_id: int) -> bool:
        """Insert data into the target database table"""
        try:
            insertion_id = insertion_config['id']
            target_database = insertion_config['target_database']
            target_table = insertion_config['target_table']
            
            # Get field mappings
            field_mappings = self.get_field_mappings(insertion_id)
            
            if not field_mappings:
                logging.warning(f"No field mappings found for insertion {insertion_id}")
                return False
            
            # Ensure table exists
            if not self.ensure_table_exists(target_database, target_table, field_mappings):
                return False
            
            # Prepare data for insertion
            insert_data = {}
            
            for mapping in field_mappings:
                field_name = mapping['target_field']
                field_value = self.resolve_field_value(mapping, email_data, extracted_data, email_id)
                
                if field_value is not None:
                    insert_data[field_name] = field_value
                else:
                    logging.warning(f"Could not resolve value for field {field_name}")
            
            if not insert_data:
                logging.warning("No data to insert")
                return False
            
            # Build and execute INSERT statement
            cursor = self.db_connection.cursor()
            cursor.execute(f"USE {target_database}")
            
            columns = list(insert_data.keys())
            values = list(insert_data.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            insert_sql = f"""
                INSERT INTO {target_table} ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            cursor.execute(insert_sql, values)
            inserted_id = cursor.lastrowid
            self.db_connection.commit()
            
            logging.info(f"Successfully inserted data into {target_database}.{target_table} with ID {inserted_id}")
            logging.debug(f"Inserted data: {insert_data}")
            
            return True
            
        except Exception as e:
            logging.error(f"Database insertion failed: {e}")
            self.db_connection.rollback()
            return False
        finally:
            cursor.close()
    
    def process_database_insertions(self, rule_id: int, email_data: Dict[str, Any], 
                                   extracted_data: Dict[str, Any], email_id: int) -> Dict[str, Any]:
        """Process all database insertions for a rule"""
        results = {
            'total_insertions': 0,
            'successful_insertions': 0,
            'failed_insertions': 0,
            'insertion_details': []
        }
        
        try:
            # Get database insertion configurations
            insertions = self.get_database_insertions(rule_id)
            results['total_insertions'] = len(insertions)
            
            for insertion in insertions:
                insertion_start_time = datetime.now()
                
                try:
                    success = self.insert_data_to_database(insertion, email_data, extracted_data, email_id)
                    
                    insertion_time = (datetime.now() - insertion_start_time).total_seconds() * 1000
                    
                    if success:
                        results['successful_insertions'] += 1
                        status = 'success'
                        error_msg = None
                    else:
                        results['failed_insertions'] += 1
                        status = 'failed'
                        error_msg = 'Insertion failed'
                    
                    results['insertion_details'].append({
                        'insertion_id': insertion['id'],
                        'target_database': insertion['target_database'],
                        'target_table': insertion['target_table'],
                        'status': status,
                        'execution_time': insertion_time,
                        'error_message': error_msg
                    })
                    
                except Exception as e:
                    results['failed_insertions'] += 1
                    insertion_time = (datetime.now() - insertion_start_time).total_seconds() * 1000
                    
                    results['insertion_details'].append({
                        'insertion_id': insertion['id'],
                        'target_database': insertion['target_database'],
                        'target_table': insertion['target_table'],
                        'status': 'failed',
                        'execution_time': insertion_time,
                        'error_message': str(e)
                    })
                    
                    logging.error(f"Database insertion failed for insertion {insertion['id']}: {e}")
            
            logging.info(f"Database insertions completed: {results['successful_insertions']}/{results['total_insertions']} successful")
            return results
            
        except Exception as e:
            logging.error(f"Database insertion processing failed: {e}")
            results['failed_insertions'] = results['total_insertions']
            return results
