#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE  ->  Drive  ->  SFTP  (multi-sites, 7 indices)

– Vide (en douceur) les dossiers Drive par site
– Calcule des composites 10 jours (LAI, NDVI, EVI, NDRE, MSAVI, SIWSI, NMDI)
– Suivi robuste des tâches Earth Engine et des .tif Drive
– Transfert SFTP puis rapport mail
"""

import os
import time
import random
import ssl
import socket
import ee
import smtplib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─────────── Secrets / variables d’environnement ────────────
SA_KEY_PATH = os.getenv("SA_KEY_PATH", "sa-key.json")

SFTP_HOST = os.environ["SFTP_HOST"]
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.environ["SFTP_USER"]
SFTP_PASS = os.environ["SFTP_PASS"]
SFTP_DEST = os.environ["SFTP_DEST_FOLDER"]

SMTP_SRV = os.environ["SMTP_SERVER"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USR = os.environ["SMTP_USER"]
SMTP_PWD = os.environ["SMTP_PASS"]
EMAILS = os.environ["ALERT_EMAILS"].split(",")

WAIT_TIME = int(os.getenv("WAIT_TIME", 3600))  # timeout EE (par site)
POLL_EVERY = 30                                # polling EE
FILE_TIMEOUT = 600                             # 10 min max pour les .tif

# ─────────── Authentification EE + Drive ────────────
creds = service_account.Credentials.from_service_account_file(
    SA_KEY_PATH,
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/earthengine",
        "https://www.googleapis.com/auth/drive",
    ],
)
ee.Initialize(credentials=creds, project=creds.project_id)
print("Earth Engine initialisé")

drv = build("drive", "v3", credentials=creds, cache_discovery=False)
print("Google Drive API initialisée")

# ─────────── Paramètres généraux ────────────
SITE_IDS = [
    'projects/gee-flow-meoss/assets/koga',
    'projects/gee-flow-meoss/assets/renk',
]

INDICES = ["NDVI", "EVI", "LAI", "NDRE", "MSAVI", "SIWSI", "NMDI"]

CLOUD_PROB_THRESHOLD = 40
EXPORT_SCALE = 10
EXPORT_CRS = "EPSG:4326"

START_DATE = ee.Date("2025-03-25")
END_DATE = ee.Date("2025-05-25")

empty_img = (
    ee.Image.constant([-32767] * len(INDICES))
    .rename(INDICES)
    .updateMask(ee.Image.constant(0))
)

# ─────────── Outils Drive (retry) ────────────
RETRY_MAX = 6
BACKOFF_BASE = 3


def _retry(fun, *args, **kwargs):
    for i in range(1, RETRY_MAX + 1):
        try:
            return fun(*args, **kwargs)
        except (ssl.SSLError, socket.error, HttpError) as exc:
            if i == RETRY_MAX:
                raise
            delay = BACKOFF_BASE * 2 ** (i - 1) * (0.5 + random.random() / 2)
            print(f"Retry {i}/{RETRY_MAX} dans {delay:.1f}s – {exc}")
            time.sleep(delay)


def drv_list(svc, **kw):
    return _retry(lambda: svc.files().list(**kw).execute())["files"]


def drv_del(svc, fid):
    try:
        _retry(lambda: svc.files().delete(fileId=fid).execute())
    except HttpError as e:
        if e.resp.status in (403, 404):
            print(f"Fichier {fid} ignoré (pas propriétaire)")
        else:
            raise


def drv_download(svc, fid, path):
    with open(path, "wb") as handle:
        req = svc.files().get_media(fileId=fid)
        downloader = MediaIoBaseDownload(handle, req)
        done = False
        while not done:
            _, done = _retry(downloader.next_chunk)


# ─────────── Fonctions Earth Engine ────────────
def mask_cloud_shadow(img):
    prob = ee.Image(img.get("cloud_prob")).select("probability")
    qa = img.select("QA60")
    mask = (
        prob.lt(CLOUD_PROB_THRESHOLD)
        .And(qa.bitwiseAnd(1 << 10).eq(0))
        .And(qa.bitwiseAnd(1 << 11).eq(0))
    )
    return img.updateMask(mask).copyProperties(img, ["system:time_start"])


def add_all_indices(img):
    b = {f"B{i}": img.select(f"B{i}").divide(1e4).toFloat() for i in [2, 3, 4, 5, 6, 8, 11, 12]}
    ndvi = b["B8"].subtract(b["B4"]).divide(b["B8"].add(b["B4"])).rename("NDVI")
    evi = (
        ee.Image(2.5)
        .multiply(
            b["B8"]
            .subtract(b["B4"])
            .divide(b["B8"].add(b["B4"].multiply(6)).add(b["B2"].multiply(-7.5)).add(1))
        )
        .rename("EVI")
    )
    lai = evi.multiply(3.618).subtract(0.118).rename("LAI")
    ndre = b["B8"].subtract(b["B5"]).divide(b["B8"].add(b["B5"])).rename("NDRE")
    msavi = (
        b["B8"]
        .multiply(2)
        .add(1)
        .subtract(
            (b["B8"].multiply(2).add(1))
            .pow(2)
            .subtract(b["B8"].subtract(b["B4"]).multiply(8))
            .sqrt()
        )
        .divide(2)
        .rename("MSAVI")
    )
    siwsi = b["B11"].subtract(b["B8"]).divide(b["B11"].add(b["B8"])).rename("SIWSI")
    nmdi = b["B8"].subtract(b["B11"].subtract(b["B12"])).divide(
        b["B8"].add(b["B11"].subtract(b["B12"]))
    ).rename("NMDI")
    return img.addBands([ndvi, evi, lai, ndre, msavi, siwsi, nmdi])


def dekad_composite(start, end, geom):
    s2sr = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(geom)
        .filterDate(start, end)
    )
    s2prob = (
        ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")
        .filterBounds(geom)
        .filterDate(start, end)
    )

    # --- filtre de jointure CORRIGÉ ---
    join_filter = ee.Filter.equals(leftField="system:index", rightField="system:index")
    join = ee.Join.saveFirst("cloud_prob")
    joined = join.apply(s2sr, s2prob, join_filter)

    col = (
        ee.ImageCollection(joined)
        .map(mask_cloud_shadow)
        .map(add_all_indices)
        .map(
            lambda im: im.addBands(
                ee.Image(im.get("cloud_prob"))
                .select("probability")
                .multiply(-1)
                .add(100)
                .rename("cloud_score")
            )
        )
    )
    return ee.Image(
        ee.Algorithms.If(col.size().gt(0), col.qualityMosaic("cloud_score").select(INDICES), empty_img)
    ).set("system:time_start", start.millis())


# ─────────── Outils SFTP ────────────
def sftp_mkdirs(sftp, path):
    current = ""
    for part in [p for p in path.split("/") if p]:
        current += "/" + part
        try:
            sftp.listdir(current)
        except IOError:
            sftp.mkdir(current)


# ─────────── Boucle principale par site ────────────
global_sent = []
global_errs = []
nb_tasks_tot = 0

for site_id in SITE_IDS:
    aoi_fc = ee.FeatureCollection(site_id)
    aoi = ee.Feature(aoi_fc.first())
    geom = aoi.geometry()
    site_name = aoi.get("Nom").getInfo() or site_id.split("/")[-1]

    print(f"\nSite : {site_name}")

    # Dossier Drive du site
    query = (
        f"name='{site_name}' "
        "and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    res = drv_list(drv, q=query, fields="files(id)")
    if not res:
        meta = {"name": site_name, "mimeType": "application/vnd.google-apps.folder"}
        folder_id = _retry(lambda: drv.files().create(body=meta, fields="id").execute())["id"]
    else:
        folder_id = res[0]["id"]
        children = drv_list(drv, q=f"'{folder_id}' in parents and trashed=false", fields="files(id)")
        for child in children:
            drv_del(drv, child["id"])

    # Exports EE
    step = 10
    period = 30
    n_days = END_DATE.difference(START_DATE, "day").getInfo()
    tasks = []

    for offset in range(0, n_days - period + 1, step):
        start = START_DATE.advance(offset, "day")
        d1 = start
        d2 = d1.advance(10, "day")
        d3 = d2.advance(10, "day")
        end = d3.advance(10, "day")
        mid = start.advance(15, "day")

        dek1 = dekad_composite(d1, d2, geom)
        dek2 = dekad_composite(d2, d3, geom)
        dek3 = dekad_composite(d3, end, geom)

        interp = dek1.add(dek3).divide(2)
        filled = dek2.where(dek2.mask().Not(), interp)

        bounded = filled.select(["NDVI", "EVI", "NDRE", "MSAVI", "SIWSI", "NMDI"]).clamp(-1, 1)
        bounded = bounded.addBands(filled.select("LAI").clamp(-1, 7))
        scaled = bounded.multiply(10000).round().toInt16()
        final_img = scaled.clip(geom).unmask(-32768).toInt16()

        date_str = mid.format("YYYYMMdd").getInfo()

        for band in INDICES:
            filename = f"{site_name}_{band}_{date_str}"
            task = ee.batch.Export.image.toDrive(
                image=final_img.select(band),
                description=filename,
                folder=site_name,
                fileNamePrefix=filename,
                region=geom,
                scale=EXPORT_SCALE,
                crs=EXPORT_CRS,
                maxPixels=1e13,
            )
            task.start()
            tasks.append(task)
            print(f"Export lancé : {filename}")

    nb_tasks_tot += len(tasks)

    # Polling EE
    pending = {t.status()["description"]: t for t in tasks}
    t0 = time.time()
    while pending and time.time() - t0 < WAIT_TIME:
        for desc, t in list(pending.items()):
            state = t.status()["state"]
            if state in ("COMPLETED", "FAILED"):
                print(f"Tâche {desc} -> {state}")
                pending.pop(desc)
        if pending:
            time.sleep(POLL_EVERY)

    if pending:
        raise RuntimeError(f"{site_name} : timeout des tâches EE")

    # Attente des .tif
    expected = len(tasks)
    t0 = time.time()
    while True:
        files = drv_list(
            drv,
            q=f"'{folder_id}' in parents and trashed=false and name contains '.tif'",
            fields="files(id,name,size)",
        )
        ready = [f for f in files if int(f.get("size", "0")) > 0]
        if len(ready) >= expected:
            break
        if time.time() - t0 > FILE_TIMEOUT:
            raise RuntimeError(f"{site_name} : fichiers .tif incomplets")
        print(f"{len(ready)}/{expected} fichiers .tif prêts...")
        time.sleep(15)

    # Transfert SFTP
    sent = []
    errs = []
    transport = Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = SFTPClient.from_transport(transport)

    remote_site_path = f"{SFTP_DEST.rstrip('/')}/{site_name}"
    sftp_mkdirs(sftp, remote_site_path)

    for f in ready:
        name = f["name"]
        fid = f["id"]
        local_tmp = f"/tmp/{name}"
        drv_download(drv, fid, local_tmp)
        try:
            sftp.put(local_tmp, f"{remote_site_path}/{name}")
            sent.append(f"{site_name}/{name}")
        except Exception as exc:
            errs.append((f"{site_name}/{name}", str(exc)))
        finally:
            os.remove(local_tmp)

    sftp.close()
    transport.close()

    global_sent.extend(sent)
    global_errs.extend(errs)
    print(f"{site_name} : {len(sent)} fichiers transférés")

# ─────────── Mail de synthèse ────────────
body_lines = [
    f"{nb_tasks_tot} exports lancés sur {len(SITE_IDS)} sites.",
    f"{len(global_sent)} fichiers transférés :\n- " + "\n- ".join(global_sent),
]
if global_errs:
    body_lines.append(
        f"{len(global_errs)} erreur(s) SFTP :\n"
        + "\n".join(f"{n} : {e}" for n, e in global_errs)
    )

msg = MIMEMultipart()
msg["From"] = SMTP_USR
msg["To"] = ",".join(EMAILS)
msg["Subject"] = "GEE -> Drive -> SFTP : " + ("Succès" if not global_errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body_lines), "plain"))

with smtplib.SMTP(SMTP_SRV, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USR, SMTP_PWD)
    server.send_message(msg)

print("Rapport mail envoyé")
