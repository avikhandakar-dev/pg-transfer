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
        # Use pg_dump to create a backup from the source DB and pipe it to psql for the target DB
        command = f"pg_dump {data.source_db_url} | psql {data.target_db_url}"
        
        # Run the command in a shell
        subprocess.run(command, shell=True, check=True, text=True)

        return {"message": "Database successfully backed up and restored"}

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Operation failed: {e.stderr or str(e)}")
