#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE  ➜  Drive  ➜  SFTP   (GitHub Actions, gratuit)
– Service‑account unique (sa‑key.json décodé depuis le secret)
– Dossier Drive vidé, réutilisé
– Polling fin de tâche EE  ➜  pas de sleep arbitraire
– Téléchargement Drive v3  ➜  SFTP (Paramiko)
– Rapport e‑mail détaillé

Dépendances :  earthengine-api  google-api-python-client  paramiko
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText, MIMEMultipart

# ────────────────────────── PARAMÈTRES MODULABLES ────────────────────────────
DRIVE_FOLDER = 'BRLi_Test_MEOSS'          # nom du dossier Drive (racine « Mon Drive »)
ASSET_SITES  = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'

SITE_NAME    = 'Rahad-2'
TILE_ID      = '36PWA'
DATE_START   = '2025-04-07'
DATE_END     = '2025-04-10'
CLOUD_MAX    = 60

INDICES      = ['NDVI', 'EVI']            # compléter si besoin
SCALE,  CRS  = 10, 'EPSG:4326'

WAIT_EE      = int(os.getenv('WAIT_TIME', 3600))   # max 1 h
POLL_EE      = 30                                  # s
POLL_DRV     = 5                                   # s

# ─────────────────────────── SECRETS (GitHub) ────────────────────────────────
SA_KEY       = os.getenv('SA_KEY_PATH', 'sa-key.json')

SFTP_HOST    = os.environ['SFTP_HOST']
SFTP_PORT    = int(os.getenv('SFTP_PORT', 22))
SFTP_USER    = os.environ['SFTP_USER']
SFTP_PASS    = os.environ['SFTP_PASS']
SFTP_DEST    = os.environ['SFTP_DEST_FOLDER']

SMTP_SERVER  = os.environ['SMTP_SERVER']
SMTP_PORT    = int(os.environ['SMTP_PORT'])
SMTP_USER    = os.environ['SMTP_USER']
SMTP_PASS    = os.environ['SMTP_PASS']
EMAILS       = os.environ['ALERT_EMAILS'].split(',')

# ─────────────────────────── INIT EE & DRIVE ────────────────────────────────
SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/earthengine',
    'https://www.googleapis.com/auth/drive',
]
cred = service_account.Credentials.from_service_account_file(SA_KEY, scopes=SCOPES)
ee.Initialize(cred, project=cred.project_id)
print("✅ EE OK")

drive = build('drive', 'v3', credentials=cred, cache_discovery=False)
print("✅ Drive API OK")

# ────────────────────────── DOSSIER DRIVE (VIDAGE) ───────────────────────────
q_folder = (f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' "
            "and 'root' in parents and trashed=false")
res = drive.files().list(q=q_folder, fields='files(id)').execute()
if res['files']:
    FID = res['files'][0]['id']
    children = drive.files().list(q=f"'{FID}' in parents and trashed=false",
                                  fields='files(id)').execute()['files']
    for ch in children:
        drive.files().delete(fileId=ch['id']).execute()
    print(f"• Dossier vidé ({len(children)})")
else:
    meta = {'name': DRIVE_FOLDER, 'mimeType': 'application/vnd.google-apps.folder'}
    FID = drive.files().create(body=meta, fields='id').execute()['id']
    print("• Dossier créé")

# ────────────────────────── COLLECTION & EXPORTS EE ──────────────────────────
geom = (ee.FeatureCollection(ASSET_SITES)
        .filter(ee.Filter.eq('SITE', SITE_NAME)).first()).geometry()

collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
              .filterDate(DATE_START, DATE_END)
              .filterBounds(geom)
              .filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'less_than', CLOUD_MAX)
              .filter(ee.Filter.eq('MGRS_TILE', TILE_ID))
              .map(lambda im: im
                   .updateMask(im.select('QA60').bitwiseAnd(1 << 10).eq(0)
                                             .And(im.select('QA60').bitwiseAnd(1 << 11).eq(0)))
                   .clip(geom))
              .map(lambda im: im.addBands([
                   im.normalizedDifference(['B8', 'B4']).rename('NDVI'),
                   ee.Image(2.5).multiply(
                       im.select('B8').subtract(im.select('B4'))
                         .divide(im.select('B8')
                                 .add(im.select('B4').multiply(6))
                                 .add(im.select('B2').multiply(-7.5))
                                 .add(1))).rename('EVI')
              ])))

ids = collection.aggregate_array('system:index').getInfo()

tasks = []
for img_id in ids:
    ee_img = collection.filter(ee.Filter.eq('system:index', img_id)).first()
    date   = ee.Date(ee_img.get('system:time_start')).format('yyyyMMdd').getInfo()
    for band in INDICES:
        desc = f"{SITE_NAME}_{TILE_ID}_{date}_{band}"
        t = ee.batch.Export.image.toDrive(
            image          = ee_img.select(band).toFloat(),
            description    = desc,
            folder         = DRIVE_FOLDER,     # nom, pas ID
            fileNamePrefix = desc,
            region         = geom,
            scale          = SCALE,
            crs            = CRS,
            maxPixels      = 1e13)
        t.start()
        tasks.append(t)
        print("🚀", desc)

# ────────────────────────── POLLING EE ───────────────────────────────────────
pending = {t.status()['description']: t for t in tasks}
start   = time.time()
print(f"⏳ {len(pending)} tâche(s) EE…")
while pending and time.time() - start < WAIT_EE:
    for name, task in list(pending.items()):
        state = task.status()['state']
        if state in ('COMPLETED', 'FAILED'):
            print(f"🔔 {name} → {state}")
            pending.pop(name)
    time.sleep(POLL_EE)
if pending:
    raise RuntimeError(f"Timeout EE ({len(pending)} tâche(s) bloquée(s))")
print("✅ Exports terminés")

# ────────────────────────── LISTE .TIF SUR DRIVE ────────────────────────────
def list_tifs():
    q = f"'{FID}' in parents and trashed=false and name contains '.tif'"
    return drive.files().list(q=q, fields='files(id,name,size)').execute()['files']

# attendre que Drive ait effectivement écrit les fichiers (taille > 0)
files = list_tifs()
while any(int(f.get('size', '0')) == 0 for f in files):
    time.sleep(POLL_DRV)
    files = list_tifs()

# ────────────────────────── DOWNLOAD & SFTP  ─────────────────────────────────
sent, errs = [], []
tr = Transport((SFTP_HOST, SFTP_PORT))
tr.connect(username=SFTP_USER, password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)

for f in files:
    name, fid = f['name'], f['id']
    tmp = f"/tmp/{name}"
    try:
        with open(tmp, 'wb') as fh:
            request = drive.files().get_media(fileId=fid)
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        sftp.put(tmp, os.path.join(SFTP_DEST, name))
        sent.append(name)
    except Exception as e:
        errs.append((name, str(e)))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)

sftp.close(); tr.close()

# ────────────────────────── RAPPORT MAIL ─────────────────────────────────────
body = [
    f"{len(tasks)} exports lancés.",
    f"{len(sent)} fichiers transférés :\n- " + ("\n- ".join(sent) if sent else "—")
]
if errs:
    body.append(f"{len(errs)} erreur(s) SFTP :\n" +
                "\n".join(f"{n}: {e}" for n, e in errs))

msg = MIMEMultipart()
msg['From'] = SMTP_USER
msg['To']   = ','.join(EMAILS)
msg['Subject'] = "GEE → Drive → SFTP : " + ("Succès" if not errs else "Succès (avec avertissements)")
msg.attach(MIMEText("\n\n".join(body), 'plain'))

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
    s.starttls()
    s.login(SMTP_USER, SMTP_PASS)
    s.send_message(msg)

print("✉️ Rapport envoyé")