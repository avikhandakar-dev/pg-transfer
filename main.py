from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import subprocess

app = FastAPI()

# API Models
class TransferRequest(BaseModel):
    source_db_url: str
    target_db_url: str

def execute_command(command: str):
    """
    Executes a shell command and raises an exception if it fails.
    """
    process = subprocess.run(command, shell=True, capture_output=True, text=True)
    if process.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Command failed: {process.stderr.strip()}"
        )
    return process.stdout.strip()

# API Endpoint
@app.post("/transfer")
async def initiate_transfer(data: TransferRequest, background_tasks: BackgroundTasks):
    """
    Backup the source database and restore it to the target database.
    """
    try:
       # Step 1: Drop and recreate the public schema in the target database
        drop_schema_command = f"psql {data.target_db_url} -c 'DROP SCHEMA public CASCADE;'"
        recreate_schema_command = f"psql {data.target_db_url} -c 'CREATE SCHEMA public;'"
        execute_command(drop_schema_command)
        execute_command(recreate_schema_command)

        # Step 2: Backup from source and restore to target
        backup_restore_command = f"pg_dump {data.source_db_url} | psql {data.target_db_url}"
        execute_command(backup_restore_command)

        return {"status": "success", "message": "Backup and restore completed successfully."}

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Operation failed: {e.stderr or str(e)}")
