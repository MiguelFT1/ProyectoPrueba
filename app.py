# app.py
import os
import json
import traceback
from utils import f as run_download

def f(event=None, context=None):
    """
    Handler para Lambda (Zappa). Lee BUCKET_NAME/PREFIX de env vars (o usa defaults),
    llama a utils.f y retorna una respuesta JSON-friendly.
    """
    bucket = os.getenv("BUCKET_NAME", "dolarrawmiguel01")
    prefix = os.getenv("PREFIX", "")

    try:
        uri = run_download(bucket=bucket, prefix=prefix)
        return {"status": "ok", "uri": uri}
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "trace": traceback.format_exc()
        }