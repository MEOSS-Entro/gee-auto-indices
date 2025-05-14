#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Earth‑Engine → Google Drive → SFTP (100 % service‑account)
- vide / crée le dossier
- exporte les indices
- attend la fin réelle des tâches
- transfère les .tif
- envoie un rapport
"""

import os, time, io, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ----------------------- PARAMÈTRES -----------------------------------------
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME      = 'Rahad-2';         TILE_ID = '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX      = 60
INDICES        = ['NDVI', 'EVI']          # compléter si besoin
EXPORT_SCALE   = 10;  EXPORT_CRS = 'EPSG:4326'
WAIT_TIME      = int(os.getenv('WAIT_TIME', 3600))
POLL_EVERY     = 30                      # s

# ----------------------- SECRETS (passés par Actions) ------------------------
SA_KEY         = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST      = os.environ['SFTP_HOST']
SFTP_PORT      = int(os.getenv('SFTP_PORT', 22))
SFTP_USER      = os.environ['SFTP_USER'];     SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST      = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER    = os.environ['SMTP_SERVER'];   SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USER      = os.environ['SMTP_USER'];     SMTP_PASS = os.environ['SMTP_PASS']
EMAILS         = os.environ['ALERT_EMAILS'].split(',')

# ----------------------- 1. INITIALISATION ----------------------------------
creds = service_account.Credentials.from_service_account_file(
    SA_KEY,
    scopes=[
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive'
    ])
ee.Initialize(credentials=creds, project=creds.project_id)
print("✅ EE initialisé.")

drive = build('drive', 'v3', credentials=creds, cache_discovery=False)
print("✅ Drive API initialisée (SA).")

# ----------------------- 2. DOSSIER DRIVE -----------------------------------
def ensure_clean_folder(name):
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = drive.files().list(q=q, fields='files(id)').execute()['files']
    if res:
        fid = res[0]['id']
        kids = drive.files().list(
            q=f"'{fid}' in parents and trashed=false", fields='files(id)'
        ).execute()['files']
        for k in kids:
            drive.files().delete(fileId=k['id']).execute()
        print(f"• Dossier vidé ({len(kids)} fichiers).")
    else:
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
        fid = drive.files().create(body=meta, fields='id').execute()['id']
        print("• Dossier créé.")
    return fid

FOLDER_ID = ensure_clean_folder(DRIVE_FOLDER)

# ----------------------- 3. IMAGE COLLECTION --------------------------------
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

# ----------------------- 4. EXPORTS -----------------------------------------
tasks = []
img_list = ic.toList(ic.size())
n_imgs   = ic.size().getInfo()

for i in range(n_imgs):
    im   = ee.Image(img_list.get(i))
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for band in INDICES:
        desc = f"{SITE_NAME}_{TILE_ID}_{date}_{band}"
        task = ee.batch.Export.image.toDrive(
            image=im.select(band).toFloat(),
            description=desc,
            folder=DRIVE_FOLDER,               # nom lisible
            fileNamePrefix=desc,
            region=geom, scale=EXPORT_SCALE,
            crs=EXPORT_CRS, maxPixels=1e13)
        task.start(); tasks.append(task); print("🚀", desc)

# ----------------------- 5. POLLING -----------------------------------------
start = time.time()
pending = {t.status()['description']: t for t in tasks}
print(f"⏳ {len(pending)} tâche(s) à attendre (timeout {WAIT_TIME}s)…")

while pending and time.time()-start < WAIT_TIME:
    for d,t in list(pending.items()):
        st = t.status()['state']
        if st in ('COMPLETED','FAILED'):
            print(f"🔔 {d} → {st}")
            pending.pop(d)
    if pending: time.sleep(POLL_EVERY)

if pending:
    raise RuntimeError("Timeout ! Tâches non terminées : " + ", ".join(pending))

print("✅ Tous les exports terminés.")

# ----------------------- 6. TÉLÉCHARGEMENT & SFTP ---------------------------
q = f"'{FOLDER_ID}' in parents and trashed=false and name contains '.tif'"
files = drive.files().list(q=q, fields='files(id,name)').execute()['files']

sent, errs = [], []
tr = Transport((SFTP_HOST, SFTP_PORT)); tr.connect(username=SFTP_USER, password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)

for f in files:
    name, fid = f['name'], f['id']
    local = f"/tmp/{name}"
    with open(local,'wb') as h:
        dl = MediaIoBaseDownload(h, drive.files().get_media(fileId=fid))
        done=False
        while not done: _,done = dl.next_chunk()
    try:
        sftp.put(local, os.path.join(SFTP_DEST, name)); sent.append(name)
    except Exception as e:
        errs.append((name,str(e)))
    finally:
        os.remove(local)

sftp.close(); tr.close()

# ----------------------- 7. RAPPORT MAIL ------------------------------------
body = [
    f"{len(tasks)} exports lancés.",
    f"{len(sent)} fichiers transférés :\n- " + "\n- ".join(sent)
]
if errs:
    body.append(f"{len(errs)} erreur(s) SFTP :\n" +
                "\n".join(f"{n}: {e}" for n,e in errs))

msg = MIMEMultipart()
msg['From'] = SMTP_USER
msg['To']   = ','.join(EMAILS)
msg['Subject'] = "GEE → Drive → SFTP : " + ("Succès" if not errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body), 'plain'))

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
print("✉️ Rapport envoyé.")