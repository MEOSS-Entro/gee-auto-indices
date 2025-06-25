#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE  ->  Drive  ->  SFTP  (multi-sites, 7 indices)
"""

import os
import time
import random
import ssl
import socket
from datetime import datetime, timedelta, timezone

import ee
import smtplib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─────────── Secrets et variables d’environnement ─────────────
SA_KEY_PATH = os.getenv("SA_KEY_PATH", "sa-key.json")
SFTP_HOST = os.environ["SFTP_HOST"]
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.environ["SFTP_USER"]
SFTP_PASS = os.environ["SFTP_PASS"]
SFTP_DEST = "/Data/PROD"
SMTP_SRV = os.environ["SMTP_SERVER"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USR = os.environ["SMTP_USER"]
SMTP_PWD = os.environ["SMTP_PASS"]
EMAILS = os.environ["ALERT_EMAILS"].split(",")
WAIT_TIME     = int(os.getenv("WAIT_TIME",     14_400))
PER_TASK_WAIT = int(os.getenv("PER_TASK_WAIT",   900))
FILE_TIMEOUT  = int(os.getenv("FILE_TIMEOUT",  1_800))
POLL_EVERY    = 30

# ─────────── Auth Earth Engine + Drive ─────────────
creds = service_account.Credentials.from_service_account_file(
    SA_KEY_PATH,
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/earthengine",
        "https://www.googleapis.com/auth/drive",
    ],
)
ee.Initialize(credentials=creds, project=creds.project_id)
drv = build("drive", "v3", credentials=creds, cache_discovery=False)

# ─────────── Paramètres de traitement ─────────────
SITE_IDS = [
    'projects/gee-flow-meoss/assets/koga',
    'projects/gee-flow-meoss/assets/kibimba',
    'projects/gee-flow-meoss/assets/kanyonyomba',
    'projects/gee-flow-meoss/assets/renk',
    'projects/gee-flow-meoss/assets/rahad',
]
INDICES = ["NDVI", "EVI", "LAI", "NDRE", "MSAVI", "SIWSI", "NMDI"]
CLOUD_PROB_THRESHOLD = 40
EXPORT_SCALE = 10
EXPORT_CRS = "EPSG:4326"
today_utc = datetime.now(timezone.utc).date()
END_DATE  = ee.Date(str(today_utc))
START_DATE = ee.Date(str(today_utc - timedelta(days=30)))
empty_img = (
    ee.Image.constant([-32767] * len(INDICES))
    .rename(INDICES)
    .updateMask(ee.Image.constant(0))
)

# ─────────── utilitaires Drive (retry réseau) ─────────────
def _retry(fun, *a, **k):
    for i in range(1, 7):
        try:
            return fun(*a, **k)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if i == 6:
                raise
            delay = 3 * 2 ** (i - 1) * (0.5 + random.random() / 2)
            print(f"Retry {i}/6 dans {delay:.1f}s – {e}")
            time.sleep(delay)

def drv_list(**kw):
    return _retry(lambda: drv.files().list(**kw).execute())["files"]

def drv_del(fid):
    try:
        _retry(lambda: drv.files().delete(fileId=fid).execute())
    except HttpError as e:
        if e.resp.status not in (403, 404):
            raise

def drv_download(fid, path):
    with open(path, "wb") as h:
        req = drv.files().get_media(fileId=fid)
        dl = MediaIoBaseDownload(h, req)
        done = False
        while not done:
            _, done = _retry(dl.next_chunk)

# ─────────── Fonctions Earth Engine ─────────────
def mask_cloud_shadow(img):
    prob = ee.Image(img.get("cloud_prob")).select("probability")
    qa = img.select("QA60")
    msk = (
        prob.lt(CLOUD_PROB_THRESHOLD)
        .And(qa.bitwiseAnd(1 << 10).eq(0))
        .And(qa.bitwiseAnd(1 << 11).eq(0))
    )
    return img.updateMask(msk).copyProperties(img, ["system:time_start"])

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
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(geom)
        .filterDate(start, end)
    )
    prob = (
        ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")
        .filterBounds(geom)
        .filterDate(start, end)
    )
    joined = ee.Join.saveFirst("cloud_prob").apply(
        s2, prob, ee.Filter.equals(leftField="system:index", rightField="system:index")
    )
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
        ee.Algorithms.If(
            col.size().gt(0), col.qualityMosaic("cloud_score").select(INDICES), empty_img
        )
    ).set("system:time_start", start.millis())

# ─────────── Aide SFTP ─────────────
def sftp_mkdirs(sftp, path):
    cur = ""
    for part in [p for p in path.split("/") if p]:
        cur += "/" + part
        try:
            sftp.listdir(cur)
        except IOError:
            sftp.mkdir(cur)

# ─────────── Boucle principale par site ─────────────
all_sent, all_errs, total_tasks = [], [], 0

for site_id in SITE_IDS:
    fc = ee.FeatureCollection(site_id)
    feat = ee.Feature(fc.first())
    geom = feat.geometry()
    site = feat.get("Nom").getInfo() or site_id.split("/")[-1]
    print(f"\n=== {site} ===")

    # Dossier Drive du site (création/vidage)
    res = drv_list(
        q=f"name='{site}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id)",
    )
    if res:
        folder_id = res[0]["id"]
    else:
        meta = {"name": site, "mimeType": "application/vnd.google-apps.folder"}
        folder_id = _retry(lambda: drv.files().create(body=meta, fields="id").execute())["id"]

    children = drv_list(
        q=f"'{folder_id}' in parents and trashed=false", fields="files(id)"
    )
    for kid in children:
        drv_del(kid["id"])
    print(f"Dossier Drive vidé ({len(children)} fichiers)")

    # Exports EE
    tasks = []
    step, period = 10, 30
    n_days = END_DATE.difference(START_DATE, "day").getInfo()
    for off in range(0, n_days - period + 1, step):
        d1 = START_DATE.advance(off, "day")
        d2 = d1.advance(10, "day")
        d3 = d2.advance(10, "day")
        d4 = d3.advance(10, "day")
        mid = d1.advance(15, "day")

        dek1, dek2, dek3 = (
            dekad_composite(d1, d2, geom),
            dekad_composite(d2, d3, geom),
            dekad_composite(d3, d4, geom),
        )
        filled = dek2.where(dek2.mask().Not(), dek1.add(dek3).divide(2))
        bounded = (
            filled.select(["NDVI", "EVI", "NDRE", "MSAVI", "SIWSI", "NMDI"]).clamp(-1, 1)
            .addBands(filled.select("LAI").clamp(-1, 7))
        )
        # Clip sur la géométrie du site (calcul rapide, comme original)
        img_clip = bounded.multiply(10000).round().toInt16().clip(geom)
        # Préparation du masque pour NoData
        site_mask = ee.Image.constant(1).clip(geom).selfMask()
        # On utilise .unmask(-32768) pour avoir -32768 hors du site (sur bbox)
        # On exporte sur la bounding box du site
        export_region = geom.bounds().getInfo()['coordinates']
        date_str = mid.format("YYYYMMdd").getInfo()
        for band in INDICES:
            # On applique le masque et l’unmask à la toute fin, bande par bande
            img_band = img_clip.select(band).updateMask(site_mask).unmask(-32768)
            fname = f"{site}_{band}_{date_str}"
            task = ee.batch.Export.image.toDrive(
                image=img_band,
                description=fname,
                folder=site,
                fileNamePrefix=fname,
                region=export_region,
                scale=EXPORT_SCALE,
                crs=EXPORT_CRS,
                maxPixels=1e13,
            )
            task.start(); tasks.append(task); print("Export lancé :", fname)

    total_tasks += len(tasks)

    # Attente des tâches EE
    site_wait = max(WAIT_TIME, PER_TASK_WAIT * len(tasks))
    pending = {t.id: t for t in tasks}
    t0 = time.time()
    while pending and time.time() - t0 < site_wait:
        for tid, t in list(pending.items()):
            if t.status()["state"] in (
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                "CANCEL_REQUESTED",
            ):
                pending.pop(tid)
        if pending:
            time.sleep(POLL_EVERY)
    if pending:
        raise RuntimeError(f"{site} : timeout EE ({len(pending)} restantes)")

    # Attente des .tif sur Drive
    want = len(tasks)
    t0 = time.time()
    while True:
        files = drv_list(
            q=f"'{folder_id}' in parents and trashed=false and name contains '.tif'",
            fields="files(id,name,size)",
        )
        ready = [f for f in files if int(f.get("size", "0")) > 0]
        if len(ready) >= want:
            break
        if time.time() - t0 > FILE_TIMEOUT:
            raise RuntimeError(f"{site} : .tif incomplets après {FILE_TIMEOUT//60} min")
        time.sleep(15)

    # Transfert SFTP sans supprimer les .tif du Drive
    sent, errs = [], []
    tr = Transport((SFTP_HOST, SFTP_PORT))
    tr.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = SFTPClient.from_transport(tr)

    remote_site = f"{SFTP_DEST.rstrip('/')}/{site}"
    sftp_mkdirs(sftp, remote_site)

    for f in ready:
        name, fid = f["name"], f["id"]
        tmp = f"/tmp/{name}"
        drv_download(fid, tmp)
        try:
            sftp.put(tmp, f"{remote_site}/{name}")
            sent.append(f"{site}/{name}")
        except Exception as e:
            errs.append((f"{site}/{name}", str(e)))
        finally:
            os.remove(tmp)

    sftp.close()
    tr.close()

    all_sent.extend(sent)
    all_errs.extend(errs)
    print(f"{site} : {len(sent)} fichiers transférés (Drive conservé)")

# ─────────── Mail de synthèse ─────────────
body = [
    f"{total_tasks} exports lancés sur {len(SITE_IDS)} sites.",
    f"{len(all_sent)} fichiers transférés :\n- " + "\n- ".join(all_sent),
]
if all_errs:
    body.append(
        f"{len(all_errs)} erreur(s) SFTP :\n"
        + "\n".join(f"{n} : {e}" for n, e in all_errs)
    )

msg = MIMEMultipart()
msg["From"] = SMTP_USR
msg["To"] = ",".join(EMAILS)
msg["Subject"] = "GEE -> Drive -> SFTP : " + ("Succès" if not all_errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body), "plain"))

with smtplib.SMTP(SMTP_SRV, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USR, SMTP_PWD)
    server.send_message(msg)

print("Rapport mail envoyé")
