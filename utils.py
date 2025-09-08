# utils.py
import time
import requests
import boto3

URL = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

def f(bucket: str, prefix: str = "") -> str:
    ts = int(time.time())
    filename = f"dolar-{ts}.json"

    r = requests.get(URL, timeout=30)  # timeout recomendado en Lambda
    r.raise_for_status()
    data = r.content

    key = f"{prefix}{filename}"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/json"
    )
    return f"s3://{bucket}/{key}"