#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE  ->  Drive  ->  FTP  (multi-sites, 7 indices) — Int16 partout
– génère les composites 10 jours (NDVI, EVI, LAI, NDRE, MSAVI, SIWSI, NMDI)
– START_DATE = aujourd’hui - 30 jours ; END_DATE = aujourd’hui (UTC)
– vide le dossier Drive de chaque site avant les exports
– conserve les .tif sur Drive après transfert FTP
– envoie un mail récapitulatif

Conventions:
- -32767 : nuages persistants (valeur valide, non NoData)
- -32768 : NoData (hors polygone), défini dans l’image ET la métadonnée GeoTIFF
- Échelle : ×10000 → Int16 pour toutes les bandes (LAI borné à 3.2 pour éviter de saturer int16 avec 32767)
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
from ftplib import FTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─────────── Secrets et variables d’environnement ───────────────────────────
SA_KEY_PATH = os.getenv("SA_KEY_PATH", "sa-key.json")

FTP_HOST = os.environ["FTP_HOST"]
FTP_PORT = int(os.getenv("FTP_PORT", 21))
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]
FTP_DEST = os.environ.get("FTP_DEST", "/upload/PROD")

SMTP_SRV = os.environ["SMTP_SERVER"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USR = os.environ["SMTP_USER"]
SMTP_PWD = os.environ["SMTP_PASS"]
EMAILS = os.environ["ALERT_EMAILS"].split(",")

WAIT_TIME     = int(os.getenv("WAIT_TIME",     14_400))  # 4 h
PER_TASK_WAIT = int(os.getenv("PER_TASK_WAIT",   900))   # 10 min / export
FILE_TIMEOUT  = int(os.getenv("FILE_TIMEOUT",  1_800))   # 30 min
POLL_EVERY    = 30

# ─────────── Auth Earth Engine + Drive ──────────────────────────────────────
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

# ─────────── Paramètres de traitement ───────────────────────────────────────
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
END_DATE  = ee.Date(str(today_utc))                       # J
START_DATE = ee.Date(str(today_utc - timedelta(days=30))) # J-30

# Image vide (masquée) pour garder l’entête des bandes
empty_img = (
    ee.Image.constant([-32767] * len(INDICES))
    .rename(INDICES)
    .updateMask(ee.Image.constant(0))
)

# ─────────── utilitaires Drive (retry réseau) ───────────────────────────────
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

# ─────────── Fonctions Earth Engine ─────────────────────────────────────────
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
    b = {f"B{i}": img.select(f"B{i}").divide(1e4).toFloat()
         for i in [2, 3, 4, 5, 6, 8, 11, 12]}
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

# ─────────── Aide FTP : création récursive de dossiers ──────────────────────
def ftp_mkdirs(ftp, path):
    dirs = path.strip('/').split('/')
    pwd = ftp.pwd()
    for d in dirs:
        if d not in ftp.nlst():
            try:
                ftp.mkd(d)
            except Exception:
                pass
        ftp.cwd(d)
    ftp.cwd(pwd)

# ─────────── Boucle principale par site ─────────────────────────────────────
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

    children = drv_list(q=f"'{folder_id}' in parents and trashed=false", fields="files(id)")
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

        dek1 = dekad_composite(d1, d2, geom)
        dek2 = dekad_composite(d2, d3, geom)
        dek3 = dekad_composite(d3, d4, geom)

        # Gap-fill central (inchangé)
        filled = dek2.where(dek2.mask().Not(), dek1.add(dek3).divide(2))

        # ===== Verrou absolu de l'ordre des bandes =====
        # (1) clamp global ±1 pour toutes les bandes
        bounded_all = filled.select(INDICES).clamp(-1, 1)
        # (2) LAI borné à [-1, 3.2] et overwrite
        lai_fixed   = filled.select('LAI').clamp(-1, 3.2)
        bounded     = bounded_all.addBands(lai_fixed, overwrite=True).select(INDICES)

        # ===== Mise à l'échelle ×10000 → Int16 (LAI inclus) =====
        scaled = bounded.multiply(10000.0).round()  # reste en float jusqu'au collage

        # 1) À l'intérieur de la ROI : vraies valeurs ; ailleurs : masqué
        #    Unmask à -32767 UNIQUEMENT pour les trous persistants à l'intérieur (valeur valide)
        img_site   = scaled.unmask(-32767)

        # Masque strict de la ROI (1 sur ROI, 0 hors ROI)
        mask_poly  = ee.Image.constant(1).clip(geom).reproject(EXPORT_CRS, None, EXPORT_SCALE)
        img_masked = img_site.updateMask(mask_poly)

        # 2) Image pleine bbox à -32768 (NoData), puis collage par MASQUE (where)
        #    => À l’intérieur de la ROI : valeurs réelles
        #       À l’extérieur de la ROI (mais dans la bbox) : -32768 (NoData)
        img_full  = ee.Image.constant([-32768] * len(INDICES)).rename(INDICES).toInt16()
        img_final = img_full.where(img_masked.mask(), img_masked.toInt16())

        date_str = mid.format("YYYYMMdd").getInfo()
        for band in INDICES:
            fname = f"{site}_{band}_{date_str}"
            task = ee.batch.Export.image.toDrive(
                image=img_final.select(band),
                description=fname,
                folder=site,
                fileNamePrefix=fname,
                region=geom.bounds().getInfo()['coordinates'],  # BBOX !
                scale=EXPORT_SCALE,
                crs=EXPORT_CRS,
                maxPixels=1e13,
                fileFormat='GeoTIFF',
                # Déclare le NoData dans la métadonnée GeoTIFF
                formatOptions={'noData': -32768}
            )
            task.start(); tasks.append(task); print("Export lancé :", fname)

    total_tasks += len(tasks)

    # Attente des tâches EE
    site_wait = max(WAIT_TIME, PER_TASK_WAIT * len(tasks))
    pending = {t.id: t for t in tasks}
    t0 = time.time()
    while pending and time.time() - t0 < site_wait:
        for tid, t in list(pending.items()):
            if t.status()["state"] in ("COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"):
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

    # Transfert FTP sans supprimer les .tif du Drive
    sent, errs = [], []
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    remote_site = f"{FTP_DEST.rstrip('/')}/{site}"
    ftp_mkdirs(ftp, remote_site)
    ftp.cwd(remote_site)

    for f in ready:
        name, fid = f["name"], f["id"]
        tmp = f"/tmp/{name}"
        drv_download(fid, tmp)
        try:
            with open(tmp, "rb") as fileobj:
                ftp.storbinary(f'STOR {name}', fileobj)
            sent.append(f"{site}/{name}")
        except Exception as e:
            errs.append((f"{site}/{name}", str(e)))
        finally:
            os.remove(tmp)

    ftp.quit()

    all_sent.extend(sent)
    all_errs.extend(errs)
    print(f"{site} : {len(sent)} fichiers transférés (Drive conservé)")

# ─────────── Mail de synthèse ───────────────────────────────────────────────
body = [
    f"{total_tasks} exports lancés sur {len(SITE_IDS)} sites.",
    f"{len(all_sent)} fichiers transférés :\n- " + "\n- ".join(all_sent),
]
if all_errs:
    body.append(
        f"{len(all_errs)} erreur(s) FTP :\n"
        + "\n".join(f"{n} : {e}" for n, e in all_errs)
    )

msg = MIMEMultipart()
msg["From"] = SMTP_USR
msg["To"] = ",".join(EMAILS)
msg["Subject"] = "GEE -> Drive -> FTP : " + ("Succès" if not all_errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body), "plain"))

with smtplib.SMTP(SMTP_SRV, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USR, SMTP_PWD)
    server.send_message(msg)

print("Rapport mail envoyé")