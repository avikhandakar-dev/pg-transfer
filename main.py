from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import subprocess

app = FastAPI()

# API Models
class TransferRequest(BaseModel):
    source_db_url: str
    target_db_url: str

# API Endpoint
@app.post("/transfer")
async def initiate_transfer(data: TransferRequest, background_tasks: BackgroundTasks):
    """
    Backup the source database and restore it to the target database.
    """
    try:
       # Drop existing schema
        drop_command = f"psql {data.target_db_url} -c \"DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; END $$;\""
        subprocess.run(drop_command, shell=True, check=True)
        
        # Perform backup and restore
        backup_command = f"pg_dump {data.source_db_url} | psql {data.target_db_url}"
        subprocess.run(backup_command, shell=True, check=True)
        return {"message": "Backup and restore completed successfully"}

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Operation failed: {e.stderr or str(e)}")
