import os
import io
import csv
import json
import datetime
import requests
from google.cloud import storage
from flask import Response

# Variables d'environnement attendues :
# RAW_BUCKET : nom du bucket GCS (ex: energy-env-raw)
# SOURCE_URL : URL Open-Meteo (Air Quality) à appeler
RAW_BUCKET = os.environ.get("RAW_BUCKET")
SOURCE_URL = os.environ.get("SOURCE_URL")


def openmeteo_to_csv(payload: bytes) -> bytes:
    """
    Convertit la réponse JSON Open-Meteo (Air Quality) en CSV tabulaire.
    Colonnes dynamiques selon les champs 'hourly' retournés.
    """
    obj = json.loads(payload)
    hourly = obj.get("hourly", {})
    times = hourly.get("time", [])

    # Toutes les clés horaires sauf 'time'
    fields = [k for k in hourly.keys() if k != "time"]

    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["time"] + fields)

    for i, t in enumerate(times):
        row = [t]
        for f in fields:
            arr = hourly.get(f, [])
            row.append(arr[i] if i < len(arr) else None)
        writer.writerow(row)

    return out.getvalue().encode("utf-8")


def ingest(request):
    """
    Cloud Function (Gen2) – Ingestion Open-Meteo vers CSV -> GCS.
    Déclenchée par HTTP. Retourne le chemin GCS écrit.
    """
    if not RAW_BUCKET or not SOURCE_URL:
        return Response(
            "Missing env vars: RAW_BUCKET and/or SOURCE_URL", status=500
        )

    # Dossier daté pour historiser les bruts
    today = datetime.datetime.utcnow().date()
    date_path = today.strftime("%Y/%m/%d")
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    try:
        # Récupération depuis Open-Meteo
        resp = requests.get(SOURCE_URL, timeout=60)
        resp.raise_for_status()

        # Conversion JSON en CSV
        csv_bytes = openmeteo_to_csv(resp.content)

        # Nom de fichier
        fname = f"openmeteo_{ts}.csv"
        gcs_path = f"raw/{date_path}/{fname}"

        # Upload vers GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(RAW_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(csv_bytes, content_type="text/csv")

        return Response(f"OK {RAW_BUCKET}/{gcs_path}", status=200)

    except Exception as e:
        # En cas d'erreur, on remonte un message utile
        return Response(f"ERROR: {type(e).__name__}: {e}", status=500)
