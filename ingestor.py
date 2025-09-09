import os, re, json, boto3, pymysql
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import unquote_plus

def _first_number(obj):
    if isinstance(obj, dict):
        for k in ("dolar","trm","valor","value","price","precio"):
            if k in obj:
                try: return Decimal(str(obj[k]).replace(",", "."))
                except InvalidOperation: pass
        for v in obj.values():
            n = _first_number(v)
            if n is not None: return n
    elif isinstance(obj, list):
        for v in obj:
            n = _first_number(v)
            if n is not None: return n
    elif isinstance(obj, (int, float, Decimal)):
        return Decimal(str(obj))
    elif isinstance(obj, str):
        try: return Decimal(obj.replace(",", "."))
        except InvalidOperation: return None
    return None

def _parse_val(b: bytes):
    t = b.decode("utf-8", "ignore")
    try:
        return _first_number(json.loads(t))
    except json.JSONDecodeError:
        import re
        m = re.search(r"(\d+(?:[.,]\d+)*)", t)
        if not m: return None
        try: return Decimal(m.group(1).replace(",", "."))
        except InvalidOperation: return None

def process_s3_event(event, context):
    print("=== Iniciando ingestor ===")
    try:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        print(f"S3 objeto: s3://{bucket}/{key}")

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        print(f"Bytes leídos de S3: {len(body)}")

        import re as _re
        m = _re.search(r"dolar-(\d+)\.json$", key)
        fechahora_utc = (datetime.fromtimestamp(int(m.group(1)), tz=timezone.utc)
                         if m else datetime.now(timezone.utc))
        print(f"fechahora (UTC): {fechahora_utc.isoformat()}")

        valor = _parse_val(body)
        print(f"valor parseado: {valor}")

        if valor is None:
            raise ValueError("No se pudo extraer 'valor' del archivo.")

        # Conexión a MySQL
        db_host = os.environ.get("DB_HOST")
        db_port = int(os.environ.get("DB_PORT", "3306"))
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        print(f"Conectando a MySQL {db_user}@{db_host}:{db_port}/{db_name}")

        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=os.environ.get("DB_PASSWORD"),
            database=db_name,
            port=db_port,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `dólar`(
                  `fechahora` DATETIME NOT NULL,
                  `valor` DECIMAL(18,6) NOT NULL,
                  PRIMARY KEY (`fechahora`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            cur.execute("""
                INSERT INTO `dólar` (`fechahora`,`valor`)
                VALUES (%s,%s)
                ON DUPLICATE KEY UPDATE `valor`=VALUES(`valor`);
            """, (fechahora_utc.replace(tzinfo=None), str(valor)))
            print("INSERT ejecutado correctamente.")
        conn.close()

        res = {"status":"ok","bucket":bucket,"key":key,
               "fechahora":fechahora_utc.isoformat(),"valor":str(valor)}
        print(f"RESULTADO: {res}")
        return res

    except Exception as e:
        import traceback
        print("ERROR en ingestor:", str(e))
        print(traceback.format_exc())
        # re-lanzar para que CloudWatch marque error
        raise