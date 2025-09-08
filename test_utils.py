import boto3
from moto import mock_aws
import utils  # <- importa desde utils.py

@mock_aws
def test_f_subida_a_s3(monkeypatch):
    # 1) S3 simulado
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)

    # 2) Evitar llamada real a BanRep
    class FakeResponse:
        def raise_for_status(self): ...
        @property
        def content(self):
            return b'{"dolar":"5000"}'

    monkeypatch.setattr("requests.get", lambda url, **kwargs: FakeResponse())

    # 3) Ejecutar f() y validar
    uri = utils.f(bucket_name)

    resp = s3.list_objects_v2(Bucket=bucket_name)
    assert "Contents" in resp
    key = resp["Contents"][0]["Key"]
    assert key.startswith("dolar-") and key.endswith(".json")

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = obj["Body"].read()
    assert body == b'{"dolar":"5000"}'
    assert uri == f"s3://{bucket_name}/{key}"