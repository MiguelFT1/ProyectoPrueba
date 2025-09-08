import boto3
import json
from datetime import datetime

def list_buckets():
    s3 = boto3.client("s3")
    resp = s3.list_buckets()

    buckets = []
    for b in resp.get("Buckets", []):
        created = b.get("CreationDate")
        created_iso = created.astimezone().isoformat() if isinstance(created, datetime) else None
        buckets.append({
            "name": b.get("Name"),
            "creation_date": created_iso
        })

    result = {
        "owner_id": resp.get("Owner", {}).get("ID"),
        "owner_display_name": resp.get("Owner", {}).get("DisplayName"),
        "count": len(buckets),
        "buckets": buckets
    }
    return result

if __name__ == "__main__":
    print(json.dumps(list_buckets(), indent=4))