#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP
• service‑account   • polling EE & Drive   • retry réseau
• rapport e‑mail    • dossier Drive vidé avant chaque run
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ───────────────────────── PARAMÈTRES GÉNÉRAUX ───────────────────────────────
DRIVE_FOLDER = 'BRLi_Test_MEOSS'          # nom EXACT du dossier partagé Drive
ASSET_SITES  = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'

SITE_NAME    = 'Rahad-2'                  # colonne SITE du FC
TILE_ID      = '36PWA'                    # code tuile Sentinel‑2
DATE_START   = '2025-04-07'
DATE_END     = '2025-04-10'
CLOUD_MAX    = 60

INDICES      = ['NDVI', 'EVI']            # ajouter si besoin
SCALE, CRS   = 10, 'EPSG:4326'

# time‑outs
WAIT_EE   = int(os.getenv('WAIT_TIME', 3600))   # max 1 h
POLL_EE   = 30                                  # s entre deux scans EE
POLL_DRV  = 5                                   # s entre deux scans Drive

# retry HTTP / SSL
RETRY_MAX = 6
BASE_DLY  = 3                                   # s

# ─────────────────────────── SECRETS (GitHub) ────────────────────────────────
SA_KEY     = os.getenv('SA_KEY_PATH', 'sa-key.json')

SFTP_HOST  = os.environ['SFTP_HOST']
SFTP_PORT  = int(os.getenv('SFTP_PORT', 22))
SFTP_USER  = os.environ['SFTP_USER']
SFTP_PASS  = os.environ['SFTP_PASS']
SFTP_DEST  = os.environ['SFTP_DEST_FOLDER']

SMTP_SERVER= os.environ['SMTP_SERVER']
SMTP_PORT  = int(os.environ['SMTP_PORT'])
SMTP_USER  = os.environ['SMTP_USER']
SMTP_PASS  = os.environ['SMTP_PASS']
EMAILS     = os.environ['ALERT_EMAILS'].split(',')

# ─────────────────────────── OUTIL RETRY ─────────────────────────────────────
def _retry(func, *a, **kw):
    for att in range(1, RETRY_MAX + 1):
        try:
            return func(*a, **kw)
        except (HttpError, ssl.SSLError, socket.error) as e:
            if att == RETRY_MAX:
                raise
            d = BASE_DLY * (2 ** (att - 1)) * (0.6 + random.random() * 0.8)
            print(f"🔄 retry {att}/{RETRY_MAX} dans {d:.1f}s ({e})")
            time.sleep(d)

# ─────────────────────────── 1. INITIALISATIONS ─────────────────────────────
scopes = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/earthengine',
    'https://www.googleapis.com/auth/drive',
]
cred = service_account.Credentials.from_service_account_file(SA_KEY, scopes=scopes)

ee.Initialize(credentials=cred, project=cred.project_id)
print("✅ EE OK")

drive = build('drive', 'v3', credentials=cred, cache_discovery=False)
print("✅ Drive API OK")

# ─────────────────────────── 2. DOSSIER DRIVE ───────────────────────────────
q = (f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' "
     "and trashed=false")
res = _retry(lambda: drive.files().list(q=q, fields='files(id)').execute())
if res['files']:
    FID = res['files'][0]['id']
    kids = _retry(lambda: drive.files().list(
        q=f"'{FID}' in parents and trashed=false", fields='files(id)').execute())['files']
    for k in kids:
        _retry(lambda: drive.files().delete(fileId=k['id']).execute())
    print(f"• Dossier vidé ({len(kids)})")
else:
    meta = {'name': DRIVE_FOLDER, 'mimeType': 'application/vnd.google-apps.folder'}
    FID = _retry(lambda: drive.files().create(body=meta, fields='id').execute())['id']
    print("• Dossier créé")

# ─────────────────────────── 3. COLLECTION EE ───────────────────────────────
geom = (ee.FeatureCollection(ASSET_SITES)
        .filter(ee.Filter.eq('SITE', SITE_NAME)).first()).geometry()

col = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
       .filterDate(DATE_START, DATE_END)
       .filterBounds(geom)
       .filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'less_than', CLOUD_MAX)
       .filter(ee.Filter.eq('MGRS_TILE', TILE_ID))
       .map(lambda im: im.updateMask(
            im.select('QA60').bitwiseAnd(1 << 10).eq(0)
              .And(im.select('QA60').bitwiseAnd(1 << 11).eq(0))).clip(geom))
       .map(lambda im: im.addBands([
            im.normalizedDifference(['B8', 'B4']).rename('NDVI'),
            ee.Image(2.5).multiply(
                im.select('B8').subtract(im.select('B4'))
                  .divide(im.select('B8')
                          .add(im.select('B4').multiply(6))
                          .add(im.select('B2').multiply(-7.5))
                          .add(1))).rename('EVI')
       ])))

# ─────────────────────────── 4. EXPORTS ──────────────────────────────────────
tasks = []
for im in col.toList(col.size()).getInfo():
    ee_im = ee.Image(im)
    d = ee.Date(ee_im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for band in INDICES:
        desc = f"{SITE_NAME}_{TILE_ID}_{d}_{band}"
        t = ee.batch.Export.image.toDrive(
            ee_im.select(band).toFloat(),
            description=desc,
            folder=DRIVE_FOLDER,
            fileNamePrefix=desc,
            region=geom,
            scale=SCALE,
            crs=CRS,
            maxPixels=1e13)
        t.start()
        tasks.append(t)
        print("🚀", desc)

# ─────────────────────────── 5. POLLING EE ───────────────────────────────────
pending = {t.status()['description']: t for t in tasks}
start = time.time()
print(f"⏳ {len(pending)} tâche(s) EE…")
while pending and time.time() - start < WAIT_EE:
    for name, task in list(pending.items()):
        state = task.status()['state']
        if state in ('COMPLETED', 'FAILED'):
            print(f"🔔 {name} → {state}")
            pending.pop(name)
    if pending:
        time.sleep(POLL_EE)
if pending:
    raise RuntimeError("Timeout EE")
print("✅ Exports terminés")

# ─────────────────────────── 6. FILES SUR DRIVE ─────────────────────────────
def wait_drive(fid):
    while True:
        size = int(_retry(lambda: drive.files().get(fileId=fid,
                                                   fields='size').execute()).get('size', '0'))
        if size > 0:
            return
        time.sleep(POLL_DRV)

q = f"'{FID}' in parents and trashed=false and name contains '.tif'"
files = _retry(lambda: drive.files().list(q=q, fields='files(id,name)').execute())['files']
for f in files:
    wait_drive(f['id'])

# ─────────────────────────── 7. DOWNLOAD & SFTP ──────────────────────────────
sent, errs = [], []
tr = Transport((SFTP_HOST, SFTP_PORT))
tr.connect(username=SFTP_USER, password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)

for f in files:
    name, fid = f['name'], f['id']
    local = f"/tmp/{name}"
    try:
        with open(local, 'wb') as h:
            req = drive.files().get_media(fileId=fid)
            dl = MediaIoBaseDownload(h, req)
            done = False
            while not done:
                _, done = _retry(dl.next_chunk)
        sftp.put(local, os.path.join(SFTP_DEST, name))
        sent.append(name)
    except Exception as e:
        errs.append((name, str(e)))
    finally:
        if os.path.exists(local):
            os.remove(local)

sftp.close(); tr.close()

# ─────────────────────────── 8. MAIL RAPPORT ────────────────────────────────
report = [
    f"{len(tasks)} exports lancés.",
    f"{len(sent)} fichiers transférés :\n- " + ("\n- ".join(sent) if sent else "—")
]
if errs:
    report.append(f"{len(errs)} erreur(s) SFTP :\n" +
                  "\n".join(f"{n}: {e}" for n, e in errs))

msg = MIMEMultipart()
msg['From'] = SMTP_USER
msg['To'] = ','.join(EMAILS)
msg['Subject'] = "GEE → Drive → SFTP : " + ("Succès" if not errs else "Succès (avec avertissements)")
msg.attach(MIMEText("\n\n".join(report), 'plain'))

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
    s.starttls()
    s.login(SMTP_USER, SMTP_PASS)
    s.send_message(msg)

print("✉️ Rapport envoyé")