import sys
import os
import sqlite3
import signal
import argparse
import logging
import logging.handlers
from datetime import datetime
from tqdm import tqdm
import re
import gzip
from collections import defaultdict, deque
import sqlparse

ctrl_c_pressed = False

def signal_handler(sig, frame):
    global ctrl_c_pressed
    print("\nCtrl+C detected. Exiting gracefully.")
    ctrl_c_pressed = True
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def setup_logging(no_log_file=False, log_rotate_size=1048576, log_level="INFO"):
    handlers = [logging.StreamHandler()]
    if not no_log_file:
        handlers.append(logging.handlers.RotatingFileHandler(
            'sqlite_to_mysql.log', 
            maxBytes=log_rotate_size, 
            backupCount=5
        ))
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger(__name__)

def escape_sql_identifier(name):
    return name.replace('`', '``')

def truncate_identifier(name, max_length=64):
    if len(name) > max_length:
        return name[:max_length-8] + f"_{hash(name) % 10000}"
    return name

def sanitize_sql_value(value):
    if isinstance(value, str):
        return ''.join(c for c in value if c.isprintable()).replace('\0', '')
    return value

def topological_sort_tables(tables, foreign_keys):
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    for table in tables:
        in_degree[table] = 0
    
    for table, fk_info in foreign_keys.items():
        for fk in fk_info:
            ref_table = fk[2]
            graph[ref_table].append(table)
            in_degree[table] += 1
    
    queue = deque([t for t in tables if in_degree[t] == 0])
    sorted_tables = []
    visited = set()
    
    while queue:
        table = queue.popleft()
        sorted_tables.append(table)
        visited.add(table)
        for neighbor in graph[table]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    if len(sorted_tables) != len(tables):
        logger.warning("Cyclic dependencies detected. Tables may need manual reordering in the SQL file.")
        sorted_tables.extend([t for t in tables if t not in sorted_tables])
    
    return sorted_tables

def sqlite_to_mysql_type(sqlite_type, max_length=None, nullable=False, max_value=None, is_primary=False):
    mapping = {
        'TEXT': 'TEXT',
        'INTEGER': 'BIGINT' if is_primary else 'INT',
        'REAL': 'DOUBLE',
        'BLOB': 'LONGBLOB',
        'NULL': 'NULL',
        'DATETIME': 'DATETIME',
        'DATE': 'DATE',
        'BOOLEAN': 'BOOLEAN',
        'NUMERIC': 'DECIMAL(10,2)',
        'DECIMAL': 'DECIMAL(10,2)'
    }
    
    sqlite_type = sqlite_type.upper()
    base_type = mapping.get(sqlite_type, sqlite_type)
    
    if sqlite_type == 'TEXT' and max_length is not None:
        if max_length > 65535:
            base_type = 'LONGTEXT'
        elif max_length > 255:
            base_type = 'TEXT'
        else:
            base_type = f'VARCHAR({min(max_length, 255)})'
    
    elif sqlite_type == 'INTEGER' and max_value is not None:
        if -2147483648 <= max_value <= 2147483647:
            base_type = 'INT'
        elif -9223372036854775808 <= max_value <= 9223372036854775807:
            base_type = 'BIGINT'
    
    if is_primary and sqlite_type == 'INTEGER':
        base_type += ' AUTO_INCREMENT'
    
    return f"{base_type}{' DEFAULT NULL' if nullable else ' NOT NULL'}"

def create_sql_dump(db_file, dump_file, drop_table=True, export_mode="both", 
                    engine="InnoDB", charset="utf8mb4", collate="utf8mb4_unicode_ci", 
                    batch_size=1000, compress=False, max_blob_size=1048576, blob_dir=None, 
                    fulltext=False, partition=None, tablespace=None, mysql_version="8.0", 
                    verify_data=False, relative_blob_paths=False):
    global logger
    logger = setup_logging()
    logger.info("Starting SQLite to MySQL conversion")
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        logger.info(f"Connected to SQLite database: {db_file}")
    except sqlite3.OperationalError as e:
        logger.error(f"Failed to connect to SQLite database: {e}")
        sys.exit(1)

    if not os.path.exists(db_file):
        logger.error(f"SQLite database file does not exist: {db_file}")
        sys.exit(1)

    try:
        if compress:
            logger.info(f"Output will be compressed as {dump_file}")
            output_file = gzip.open(dump_file, 'wt', encoding='utf-8')
        else:
            output_file = open(dump_file, 'w', encoding='utf-8')
    except OSError as e:
        logger.error(f"Failed to open output file {dump_file}: {e}")
        sys.exit(1)
    
    # Get all items and sort by dependencies
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'view', 'trigger');")
    items = cursor.fetchall()
    tables = [item[0] for item in items if item[0] != 'sqlite_sequence' and item[1] and item[1].lower().startswith('create table')]
    table_dependencies = {}
    for table in tables:
        cursor.execute(f"PRAGMA foreign_key_list(`{table}`);")
        table_dependencies[table] = cursor.fetchall()
    
    sorted_tables = topological_sort_tables(tables, table_dependencies)
    sorted_items = []
    for name, sql in items:
        if name == 'sqlite_sequence':
            continue
        if sql and sql.lower().startswith('create table') and name in sorted_tables:
            sorted_items.append((name, sql, sorted_tables.index(name)))
        else:
            sorted_items.append((name, sql, len(sorted_tables)))
    sorted_items.sort(key=lambda x: x[2])
    
    total_items = len(sorted_items)
    logger.info(f"Found {total_items} items (tables, views, triggers) to export")

    with output_file as f:
        f.write(f"-- SQL Dump generated by sqlite_to_mysql.py\n")
        f.write(f"-- Source DB: {db_file}\n")
        f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
        f.write("START TRANSACTION;\n\n")

        pbar_total = tqdm(total=total_items, desc="Overall Progress")
        for idx, (item_name, item_sql, _) in enumerate(sorted_items, 1):
            if item_name == 'sqlite_sequence':
                continue

            logger.info(f"Processing item: {item_name} ({idx}/{total_items})")
            pbar_total.update(1)

            # Handle views
            if item_sql and item_sql.lower().startswith('create view'):
                if export_mode in ("structure", "both"):
                    f.write(f"\n-- View: {escape_sql_identifier(item_name)}\n")
                    view_sql = re.sub(r'CREATE\s+VIEW\s+"?(\w+)"?\s+AS\s+(.+)', 
                                      r'CREATE VIEW `\1` AS \2', item_sql, flags=re.IGNORECASE)
                    f.write(f"{view_sql};\n")
                continue

            # Handle triggers
            if item_sql and item_sql.lower().startswith('create trigger'):
                if export_mode in ("structure", "both"):
                    f.write(f"\n-- Trigger: {escape_sql_identifier(item_name)}\n")
                    f.write(f"-- Warning: Trigger may need manual adjustment for MySQL compatibility\n")
                    formatted_sql = sqlparse.format(item_sql, reindent=True)
                    formatted_sql = formatted_sql.replace("RAISE(ABORT, ", "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ")
                    formatted_sql = formatted_sql.replace("RAISE(ROLLBACK, ", "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ")
                    formatted_sql = formatted_sql.replace("RAISE(FAIL, ", "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ")
                    f.write(f"{formatted_sql};\n")
                continue

            # Process tables
            cursor.execute(f"PRAGMA table_info(`{item_name}`);")
            columns_info = cursor.fetchall()

            primary_keys = [col[1] for col in columns_info if col[5] > 0]
            column_metadata = {}
            for col in columns_info:
                col_name = col[1]
                col_type = col[2].upper() if col[2] else 'TEXT'
                notnull = col[3]
                default_value = col[4]
                is_primary = col[5] > 0
                
                metadata = {
                    'type': col_type,
                    'nullable': not notnull,
                    'is_primary': is_primary,
                    'default_value': default_value,
                    'max_length': 0,
                    'max_value': 0
                }
                
                try:
                    if col_type == 'TEXT':
                        cursor.execute(f"SELECT MAX(LENGTH(`{col_name}`)) FROM `{item_name}`;")
                        max_length = cursor.fetchone()[0] or 0
                        metadata['max_length'] = max_length
                    elif col_type == 'INTEGER':
                        cursor.execute(f"SELECT MAX(`{col_name}`) FROM `{item_name}`;")
                        max_value = cursor.fetchone()[0] or 0
                        metadata['max_value'] = max_value
                except sqlite3.Error as e:
                    logger.warning(f"Could not calculate max length/value for {col_name}: {e}")
                
                column_metadata[col_name] = metadata

            if export_mode in ("structure", "both"):
                if drop_table:
                    f.write(f"DROP TABLE IF EXISTS `{escape_sql_identifier(item_name)}`;\n")

                columns = []
                for col in columns_info:
                    col_name = col[1]
                    meta = column_metadata[col_name]
                    mysql_type = sqlite_to_mysql_type(
                        meta['type'],
                        meta['max_length'],
                        meta['nullable'],
                        meta['max_value'],
                        meta['is_primary']
                    )
                    if meta['default_value'] and meta['default_value'] != '':
                        if meta['default_value'].upper() == 'NULL':
                            mysql_type += " DEFAULT NULL"
                        elif meta['type'] in ('TEXT', 'DATETIME', 'DATE'):
                            default = sanitize_sql_value(meta['default_value'])
                            if re.match(r'^\w+\(.*\)$', default):
                                logger.warning(f"Skipping complex default value for {col_name}: {default}")
                            else:
                                mysql_type += f" DEFAULT '{default}'"
                        else:
                            mysql_type += f" DEFAULT {meta['default_value']}"
                    columns.append(f"`{escape_sql_identifier(col_name)}` {mysql_type}")

                if primary_keys:
                    pk_columns = ', '.join([f"`{escape_sql_identifier(pk)}`" for pk in primary_keys])
                    columns.append(f"PRIMARY KEY ({pk_columns})")

                table_options = []
                if partition:
                    table_options.append(partition)
                if tablespace:
                    table_options.append(f"TABLESPACE {tablespace}")

                f.write(f"CREATE TABLE `{escape_sql_identifier(item_name)}` (\n")
                f.write(f"  {',\n  '.join(columns)}\n")
                f.write(f") ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={collate}")
                if table_options:
                    f.write(" " + " ".join(table_options))
                f.write(";\n\n")

                # Export indexes
                cursor.execute(f"PRAGMA index_list(`{item_name}`);")
                indexes = cursor.fetchall()
                for index in indexes:
                    index_name = truncate_identifier(index[1])
                    unique = "UNIQUE " if index[2] else ""
                    cursor.execute(f"PRAGMA index_info(`{index_name}`);")
                    index_columns = [col[2] for col in cursor.fetchall()]
                    if index_name.startswith('sqlite_autoindex'):
                        index_name = truncate_identifier(f"idx_{item_name}_{'_'.join(index_columns)}_{hash(index_name) % 10000}")
                    columns_sql = ', '.join([f"`{escape_sql_identifier(col)}`" for col in index_columns])
                    f.write(f"CREATE {unique}INDEX `{escape_sql_identifier(index_name)}` "
                            f"ON `{escape_sql_identifier(item_name)}` ({columns_sql});\n")

                # Add full-text indexes if requested
                if fulltext and any(meta['type'] == 'TEXT' for meta in column_metadata.values()):
                    if mysql_version.startswith('5') and engine != 'MyISAM':
                        logger.warning(f"FULLTEXT indexes are only supported with MyISAM in MySQL 5.x for table {item_name}")
                    else:
                        fulltext_cols = [f"`{escape_sql_identifier(col)}`" for col, meta in column_metadata.items() if meta['type'] == 'TEXT']
                        if fulltext_cols:
                            f.write(f"CREATE FULLTEXT INDEX `fulltext_{item_name}` "
                                    f"ON `{escape_sql_identifier(item_name)}` ({', '.join(fulltext_cols)});\n")

                # Export foreign keys from pre-fetched dependencies
                fk_list = table_dependencies.get(item_name, [])
                for fk in fk_list:
                    fk_column = fk[3]
                    ref_table = fk[2]
                    ref_column = fk[4]
                    on_delete = fk[5] if fk[5] else 'NO ACTION'
                    on_update = fk[6] if fk[6] else 'NO ACTION'
                    constraint_name = truncate_identifier(f"fk_{item_name}_{fk_column}_{hash(fk_column) % 10000}")
                    f.write(
                        f"ALTER TABLE `{escape_sql_identifier(item_name)}` ADD CONSTRAINT "
                        f"`{constraint_name}` FOREIGN KEY (`{escape_sql_identifier(fk_column)}`) "
                        f"REFERENCES `{escape_sql_identifier(ref_table)}` (`{escape_sql_identifier(ref_column)}`) "
                        f"ON DELETE {on_delete} ON UPDATE {on_update};\n"
                    )
                f.write("\n")

            if export_mode in ("data", "both"):
                cursor.execute(f"SELECT COUNT(*) FROM `{item_name}`;")
                total_rows = cursor.fetchone()[0]
                
                if total_rows == 0:
                    logger.info(f"Skipping empty table: {item_name}")
                    continue
                
                logger.info(f"Exporting {total_rows} rows from {item_name}")
                columns_names = ', '.join([f"`{escape_sql_identifier(col[1])}`" for col in columns_info])
                
                offset = 0
                pbar = tqdm(total=total_rows, desc=f"Exporting {item_name}")
                values_list = []
                
                while offset < total_rows:
                    if ctrl_c_pressed:
                        logger.info("Ctrl+C detected. Exiting gracefully.")
                        sys.exit(0)
                    
                    cursor.execute(f"SELECT * FROM `{item_name}` LIMIT {batch_size} OFFSET {offset};")
                    row_count = 0
                    for row in cursor:
                        values = []
                        for i, col_value in enumerate(row):
                            col_name = columns_info[i][1]
                            col_type = column_metadata[col_name]['type']
                            
                            try:
                                if col_value is None:
                                    values.append('NULL')
                                elif col_type == 'BLOB' and isinstance(col_value, bytes):
                                    if blob_dir and len(col_value) > max_blob_size:
                                        blob_filename = f"{item_name}_{col_name}_{offset}_{i}.bin"
                                        blob_path = os.path.join(blob_dir, blob_filename)
                                        os.makedirs(blob_dir, exist_ok=True)
                                        try:
                                            with open(blob_path, 'wb') as blob_file:
                                                blob_file.write(col_value)
                                            if not os.path.exists(blob_path):
                                                logger.warning(f"Failed to write BLOB file {blob_path}")
                                                values.append('NULL')
                                            else:
                                                if relative_blob_paths:
                                                    rel_path = os.path.relpath(blob_path, os.path.dirname(dump_file))
                                                    values.append(f"LOAD_FILE('{rel_path}')")
                                                else:
                                                    values.append(f"LOAD_FILE('{blob_path}')")
                                        except OSError as e:
                                            logger.warning(f"Error writing BLOB file {blob_path}: {e}")
                                            values.append('NULL')
                                    elif len(col_value) > max_blob_size:
                                        logger.warning(f"Skipping large BLOB in {col_name} (size: {len(col_value)} bytes)")
                                        values.append('NULL')
                                    else:
                                        values.append(f"UNHEX('{col_value.hex()}')")
                                elif isinstance(col_value, str):
                                    sanitized = sanitize_sql_value(col_value)
                                    values.append(f"'{sanitized.replace(\"'\", \"''\")}'")
                                elif isinstance(col_value, (int, float)):
                                    values.append(str(col_value))
                                else:
                                    sanitized = sanitize_sql_value(str(col_value))
                                    values.append(f"'{sanitized.replace(\"'\", \"''\")}'")
                            except Exception as e:
                                logger.warning(f"Error processing value for {col_name}: {e}")
                                values.append('NULL')
                        
                        values_list.append(f"({', '.join(values)})")
                        row_count += 1
                    
                    if values_list:
                        for i in range(0, len(values_list), 100):
                            chunk = values_list[i:i+100]
                            insert_sql = (
                                f"INSERT INTO `{escape_sql_identifier(item_name)}` ({columns_names}) VALUES\n"
                                f"  {',\n  '.join(chunk)};\n"
                            )
                            f.write(insert_sql)
                        values_list = []
                    
                    offset += row_count
                    pbar.update(row_count)
                
                pbar.close()
                if verify_data:
                    cursor.execute(f"SELECT COUNT(*) FROM `{item_name}`;")
                    actual_rows = cursor.fetchone()[0]
                    if actual_rows != total_rows:
                        logger.warning(f"Data verification failed for {item_name}: expected {total_rows}, found {actual_rows}")
                logger.info(f"Exported {total_rows} rows from {item_name}")

        f.write("\nCOMMIT;\n")
        f.write("SET FOREIGN_KEY_CHECKS = 1;\n")
    
    pbar_total.close()
    conn.close()
    logger.info(f"Conversion completed successfully. Output file: {dump_file}{'.gz' if compress else ''}")

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Convert SQLite database to MySQL SQL dump",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("sqlite_db_file", help="Path to SQLite database file")
    parser.add_argument("mysql_dump_file", nargs="?", default=None,
                       help="Output MySQL dump file path")
    parser.add_argument("--no-drop", action="store_true",
                       help="Skip DROP TABLE statements")
    parser.add_argument("--export-mode", choices=["structure", "data", "both"], 
                       default="both", help="Export mode")
    parser.add_argument("--engine", default="InnoDB", 
                       help="MySQL storage engine")
    parser.add_argument("--charset", default="utf8mb4", 
                       help="MySQL character set")
    parser.add_argument("--collate", default="utf8mb4_unicode_ci", 
                       help="MySQL collation")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Batch size for data export")
    parser.add_argument("--no-log-file", action="store_true",
                       help="Disable logging to file")
    parser.add_argument("--compress", action="store_true",
                       help="Compress output SQL file using gzip")
    parser.add_argument("--max-blob-size", type=int, default=1048576,
                       help="Maximum BLOB size in bytes before skipping or externalizing")
    parser.add_argument("--blob-dir", type=str, default=None,
                       help="Directory to store large BLOBs")
    parser.add_argument("--fulltext", action="store_true",
                       help="Add FULLTEXT indexes for TEXT columns")
    parser.add_argument("--partition", type=str, default=None,
                       help="MySQL partitioning clause")
    parser.add_argument("--tablespace", type=str, default=None,
                       help="MySQL tablespace name")
    parser.add_argument("--mysql-version", default="8.0",
                       help="Target MySQL version (e.g., 5.7, 8.0)")
    parser.add_argument("--verify-data", action="store_true",
                       help="Verify row counts after export")
    parser.add_argument("--relative-blob-paths", action="store_true",
                       help="Use relative paths for BLOB files in LOAD_FILE")
    parser.add_argument("--log-rotate-size", type=int, default=1048576,
                       help="Max log file size in bytes before rotation")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       default="INFO", help="Logging level")
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    VALID_ENGINES = ['InnoDB', 'MyISAM', 'MEMORY']
    VALID_CHARSETS = ['utf8mb4', 'utf8', 'latin1']
    VALID_COLLATES = ['utf8mb4_unicode_ci', 'utf8_general_ci', 'latin1_swedish_ci']
    
    setup_logging(no_log_file=args.no_log_file, log_rotate_size=args.log_rotate_size, log_level=args.log_level)
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(args.sqlite_db_file):
        logger.error(f"SQLite database file does not exist: {args.sqlite_db_file}")
        sys.exit(1)
    
    if args.engine not in VALID_ENGINES:
        logger.error(f"Invalid engine: {args.engine}. Valid options: {', '.join(VALID_ENGINES)}")
        sys.exit(1)
    if args.charset not in VALID_CHARSETS:
        logger.error(f"Invalid charset: {args.charset}. Valid options: {', '.join(VALID_CHARSETS)}")
        sys.exit(1)
    if args.collate not in VALID_COLLATES:
        logger.error(f"Invalid collate: {args.collate}. Valid options: {', '.join(VALID_COLLATES)}")
        sys.exit(1)
    
    if args.fulltext and args.mysql_version.startswith('5') and args.engine != 'MyISAM':
        logger.error("FULLTEXT indexes are only supported with MyISAM in MySQL 5.x")
        sys.exit(1)
    
    if not args.mysql_dump_file:
        base_name = os.path.splitext(args.sqlite_db_file)[0]
        args.mysql_dump_file = f"{base_name}_mysql.sql"
        if args.compress:
            args.mysql_dump_file += '.gz'
    
    if args.blob_dir and not os.path.exists(args.blob_dir):
        os.makedirs(args.blob_dir, exist_ok=True)
    
    create_sql_dump(
        args.sqlite_db_file,
        args.mysql_dump_file,
        drop_table=not args.no_drop,
        export_mode=args.export_mode,
        engine=args.engine,
        charset=args.charset,
        collate=args.collate,
        batch_size=args.batch_size,
        compress=args.compress,
        max_blob_size=args.max_blob_size,
        blob_dir=args.blob_dir,
        fulltext=args.fulltext,
        partition=args.partition,
        tablespace=args.tablespace,
        mysql_version=args.mysql_version,
        verify_data=args.verify_data,
        relative_blob_paths=args.relative_blob_paths
  )
