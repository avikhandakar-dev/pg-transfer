import psycopg2
from psycopg2.extras import DictCursor
import logging
from datetime import datetime
from urllib.parse import urlparse, unquote
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'db_transfer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

app = FastAPI()

# Database connection
def connect_db(db_url):
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# Fetch all tables
def get_all_tables(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            return [table[0] for table in cur.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching tables: {str(e)}")
        return []

# Fetch table schema
def get_table_schema(conn, table):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE lower(table_name) = lower(%s) AND table_schema = 'public'
            """, (table,))
            exact_table_name = cur.fetchone()
            if not exact_table_name:
                return None
            exact_table_name = exact_table_name[0]
            
            cur.execute(f"""
                SELECT column_name, data_type, character_maximum_length,
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE lower(table_name) = lower(%s) AND table_schema = 'public'
            """, (table,))
            columns = cur.fetchall()
            
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = 'public'
                AND lower(tc.table_name) = lower(%s)
            """, (table,))
            primary_keys = [pk[0] for pk in cur.fetchall()]
            
            create_stmt = f'CREATE TABLE IF NOT EXISTS "{exact_table_name}" (\n'
            column_defs = []
            
            for col in columns:
                name, dtype, max_length, nullable, default = col
                col_def = f'    "{name}" {dtype}'
                if max_length:
                    col_def += f"({max_length})"
                if nullable != 'YES':
                    col_def += " NOT NULL"
                if default:
                    col_def += f" DEFAULT {default}"
                column_defs.append(col_def)
            
            if primary_keys:
                column_defs.append(f'    PRIMARY KEY ({", ".join(primary_keys)})')
                
            create_stmt += ',\n'.join(column_defs)
            create_stmt += "\n);"
            
            return create_stmt
    except Exception as e:
        logging.error(f"Error getting schema for table {table}: {str(e)}")
        return None

# Copy table data
def copy_table_data(source_conn, target_conn, table):
    try:
        with source_conn.cursor(cursor_factory=DictCursor) as source_cur, \
             target_conn.cursor() as target_cur:
            source_cur.execute(f'SELECT * FROM "{table}" LIMIT 0')
            columns = [desc[0] for desc in source_cur.description]
            
            chunk_size = 5000
            source_cur.execute(f'SELECT * FROM "{table}"')
            while rows := source_cur.fetchmany(chunk_size):
                columns_str = ', '.join(f'"{col}"' for col in columns)
                values_template = ','.join(['%s'] * len(columns))
                insert_query = f'INSERT INTO "{table}" ({columns_str}) VALUES ({values_template})'
                target_cur.executemany(insert_query, rows)
                target_conn.commit()
                logging.info(f"Inserted {len(rows)} rows into {table}")
    except Exception as e:
        logging.error(f"Error copying data for table {table}: {str(e)}")
        raise

# Main transfer function
def transfer_database(source_url, target_url):
    logging.info("Starting database transfer")
    source_conn = connect_db(source_url)
    target_conn = connect_db(target_url)
    try:
        tables = get_all_tables(source_conn)
        for table in tables:
            try:
                create_stmt = get_table_schema(source_conn, table)
                if not create_stmt:
                    logging.warning(f"Skipping table {table}: schema not found")
                    continue
                with target_conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                    cur.execute(create_stmt)
                    target_conn.commit()
                copy_table_data(source_conn, target_conn, table)
            except Exception as e:
                logging.error(f"Error processing table {table}: {str(e)}")
                target_conn.rollback()  # Rollback for the current table
    finally:
        source_conn.close()
        target_conn.close()
        logging.info("Database transfer completed")

# API Models
class TransferRequest(BaseModel):
    source_db_url: str
    target_db_url: str

# API Endpoint
@app.post("/transfer")
async def initiate_transfer(request: TransferRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(transfer_database, request.source_db_url, request.target_db_url)
    return {"message": "Database transfer initiated. Check logs for progress."}
