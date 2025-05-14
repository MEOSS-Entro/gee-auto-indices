#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Earth‑Engine → Google Drive → SFTP (100 % service‑account, gratuit)
"""

import os, time, io, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------------------------------------------------------------------------
# PARAMÈTRES
# ---------------------------------------------------------------------------
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'            # nom EXACT du dossier partagé
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME      = 'Rahad-2'
TILE_ID        = '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX      = 60
INDICES        = ['NDVI', 'EVI']              # ajouter la liste complète si besoin
EXPORT_SCALE   = 10
EXPORT_CRS     = 'EPSG:4326'
WAIT_TIME      = int(os.getenv('WAIT_TIME', 3600))
POLL_EVERY     = 30

# ---------------------------------------------------------------------------
# SECRETS (passés par le workflow)
# ---------------------------------------------------------------------------
SA_KEY         = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST      = os.environ['SFTP_HOST']
SFTP_PORT      = int(os.getenv('SFTP_PORT', 22))
SFTP_USER      = os.environ['SFTP_USER']
SFTP_PASS      = os.environ['SFTP_PASS']
SFTP_DEST      = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER    = os.environ['SMTP_SERVER']
SMTP_PORT      = int(os.environ['SMTP_PORT'])
SMTP_USER      = os.environ['SMTP_USER']
SMTP_PASS      = os.environ['SMTP_PASS']
EMAILS         = os.environ['ALERT_EMAILS'].split(',')

# ---------------------------------------------------------------------------
# 1. Initialisation Earth‑Engine & Drive (google‑auth)
# ---------------------------------------------------------------------------
creds = service_account.Credentials.from_service_account_file(
    SA_KEY,
    scopes=[
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive']
)
ee.Initialize(credentials=creds, project=creds.project_id)
print("✅ EE initialisé.")

drive_svc = build('drive', 'v3', credentials=creds, cache_discovery=False)
print("✅ Drive API initialisée (service‑account).")

# ---------------------------------------------------------------------------
# 2. Créer / vider le dossier Drive
# ---------------------------------------------------------------------------
def ensure_clean_folder(name):
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = drive_svc.files().list(q=q, fields='files(id)').execute()['files']
    if res:
        fid = res[0]['id']
        kids = drive_svc.files().list(
            q=f"'{fid}' in parents and trashed=false", fields='files(id)').execute()['files']
        for k in kids:
            drive_svc.files().delete(fileId=k['id']).execute()
        print(f"• Dossier vidé ({len(kids)} fichiers).")
    else:
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
        fid = drive_svc.files().create(body=meta, fields='id').execute()['id']
        print("• Dossier créé.")
    return fid

FOLDER_ID = ensure_clean_folder(DRIVE_FOLDER)

# ---------------------------------------------------------------------------
# 3. Lancement des exports
# ---------------------------------------------------------------------------
feat = (ee.FeatureCollection(ASSET_SITES)
        .filter(ee.Filter.eq('SITE', SITE_NAME)).first())
geom = feat.geometry()

ic = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
      .filterDate(DATE_START, DATE_END)
      .filterBounds(geom)
      .filterMetadata('CLOUDY_PIXEL_PERCENTAGE','less_than',CLOUD_MAX)
      .filter(ee.Filter.eq('MGRS_TILE', TILE_ID))
      .map(lambda img: img.updateMask(
            img.select('QA60').bitwiseAnd(1<<10).eq(0)
          .And(img.select('QA60').bitwiseAnd(1<<11).eq(0))).clip(geom))
      .map(lambda img: img.addBands([
          img.normalizedDifference(['B8','B4']).rename('NDVI'),
          ee.Image(2.5).multiply(
              img.select('B8').subtract(img.select('B4'))
                 .divide(img.select('B8').add(img.select('B4').multiply(6))
                         .add(img.select('B2').multiply(-7.5))
                         .add(1))).rename('EVI')
      ])))

tasks = []
for img_info in ic.toList(ic.size()).getInfo():
    im  = ee.Image(img_info)
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for band in INDICES:
        desc = f"{SITE_NAME}_{TILE_ID}_{date}_{band}"
        task = ee.batch.Export.image.toDrive(
            im.select(band).toFloat(),
            description=desc,
            folder=DRIVE_FOLDER,          # nom du dossier, PAS l'ID
            fileNamePrefix=desc,
            region=geom,
            scale=EXPORT_SCALE,
            crs=EXPORT_CRS,
            maxPixels=1e13)
        task.start(); tasks.append(task); print("🚀", desc)

# ---------------------------------------------------------------------------
# 4. Polling
# ---------------------------------------------------------------------------
start = time.time()
pending = {t.status()['description']: t for t in tasks}
while pending and time.time()-start < WAIT_TIME:
    for desc, t in list(pending.items()):
        st = t.status()['state']
        if st in ('COMPLETED', 'FAILED'):
            print(f"🔔 {desc} → {st}")
            pending.pop(desc)
    if pending: time.sleep(POLL_EVERY)
if pending:
    raise RuntimeError(f"Tâches non terminées : {list(pending)}")
print("✅ Tous les exports terminés.")

# ---------------------------------------------------------------------------
# 5. Téléchargement & SFTP
# ---------------------------------------------------------------------------
query = f"'{FOLDER_ID}' in parents and trashed=false and name contains '.tif'"
files = drive_svc.files().list(q=query, fields='files(id,name)').execute()['files']

ok, err = [], []
tr = Transport((SFTP_HOST, SFTP_PORT)); tr.connect(username=SFTP_USER, password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)

for f in files:
    name, fid = f['name'], f['id']
    local = f"/tmp/{name}"
    with open(local, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, drive_svc.files().get_media(fileId=fid))
        done = False
        while not done:
            _, done = downloader.next_chunk()
    try:
        sftp.put(local, os.path.join(SFTP_DEST, name)); ok.append(name)
    except Exception as e:
        err.append((name, str(e)))
    finally:
        os.remove(local)

sftp.close(); tr.close()

# ---------------------------------------------------------------------------
# 6. Rapport mail
# ---------------------------------------------------------------------------
body = [
    f"{len(tasks)} exports lancés.",
    f"{len(ok)} fichiers transférés :\n- " + "\n- ".join(ok)
]
if err:
    body.append(f"{len(err)} erreur(s) SFTP :\n" +
                "\n".join(f"{n}: {e}" for n,e in err))

msg = MIMEMultipart()
msg['From'] = SMTP_USER
msg['To'] = ','.join(EMAILS)
msg['Subject'] = "GEE → Drive → SFTP : " + ("Succès" if not err else "Terminé avec erreurs")
msg.attach(MIMEText("\n\n".join(body), 'plain'))

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
print("✉️ Rapport envoyé.")