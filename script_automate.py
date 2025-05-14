#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP avec reprise automatique réseau + rapport mail
• service‑account unique     • pas de Cloud Storage payant
• polling des tâches EE      • retry SSL/HTTP sur Drive
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------- PARAMÈTRES ------------------------------------------------------
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME      = 'Rahad-2'; TILE_ID = '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX      = 60
INDICES        = ['NDVI', 'EVI']
EXPORT_SCALE   = 10; EXPORT_CRS = 'EPSG:4326'
WAIT_TIME      = int(os.getenv('WAIT_TIME', 3600))
POLL_EVERY     = 30

# ---------- SECRETS (env) ----------------------------------------------------
SA_KEY         = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST      = os.environ['SFTP_HOST']; SFTP_PORT = int(os.getenv('SFTP_PORT',22))
SFTP_USER      = os.environ['SFTP_USER']; SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST      = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER    = os.environ['SMTP_SERVER']; SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USER      = os.environ['SMTP_USER'];   SMTP_PASS = os.environ['SMTP_PASS']
EMAILS         = os.environ['ALERT_EMAILS'].split(',')

# ---------- OUTILS DRIVE avec retry -----------------------------------------
RETRY_MAX  = 6; BASE_DELAY = 3
def _retry(fn, *args, **kwargs):
    for a in range(1, RETRY_MAX+1):
        try:
            return fn(*args, **kwargs)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if a == RETRY_MAX: raise
            d = BASE_DELAY*(2**(a-1))*(0.5+random.random()/2)
            print(f"🔄 Retry {a}/{RETRY_MAX} dans {d:.1f}s -> {e}")
            time.sleep(d)

def drv_list(svc, **kwargs):
    return _retry(lambda: svc.files().list(**kwargs).execute())['files']

def drv_delete(svc, fid):
    _retry(lambda: svc.files().delete(fileId=fid).execute())

def drv_download(svc, fid, local_path):
    with open(local_path,'wb') as h:
        req = svc.files().get_media(fileId=fid)
        dl  = MediaIoBaseDownload(h, req)
        done=False
        while not done:
            _, done = _retry(dl.next_chunk)

# ---------- 1. INITIALISATION -----------------------------------------------
creds = service_account.Credentials.from_service_account_file(
    SA_KEY,
    scopes=[
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive'
    ])
ee.Initialize(credentials=creds, project=creds.project_id); print("✅ EE OK")
drv = build('drive', 'v3', credentials=creds, cache_discovery=False); print("✅ Drive OK")

# ---------- 2. DOSSIER DRIVE -------------------------------------------------
q = f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
res = drv_list(drv, q=q, fields='files(id)')
if res:
    fid = res[0]['id']
    kids = drv_list(drv, q=f"'{fid}' in parents and trashed=false", fields='files(id)')
    for k in kids: drv_delete(drv, k['id'])
    print(f"• Dossier vidé ({len(kids)})")
else:
    meta={'name':DRIVE_FOLDER,'mimeType':'application/vnd.google-apps.folder'}
    fid = _retry(lambda: drv.files().create(body=meta, fields='id').execute())['id']
    print("• Dossier créé")
FOLDER_ID = fid

# ---------- 3. COLLECTION & EXPORTS -----------------------------------------
geom = (ee.FeatureCollection(ASSET_SITES)
          .filter(ee.Filter.eq('SITE', SITE_NAME)).first()).geometry()

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

tasks=[]; img_list=ic.toList(ic.size()); n=ic.size().getInfo()
for i in range(n):
    im   = ee.Image(img_list.get(i))
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for b in INDICES:
        desc=f"{SITE_NAME}_{TILE_ID}_{date}_{b}"
        t=ee.batch.Export.image.toDrive(im.select(b).toFloat(),
                description=desc, folder=DRIVE_FOLDER, fileNamePrefix=desc,
                region=geom, scale=EXPORT_SCALE, crs=EXPORT_CRS, maxPixels=1e13)
        t.start(); tasks.append(t); print("🚀",desc)

# ---------- 4. POLLING -------------------------------------------------------
start=time.time(); pend={t.status()['description']:t for t in tasks}
print(f"⏳ {len(pend)} tâche(s)…")
while pend and time.time()-start<WAIT_TIME:
    for d,t in list(pend.items()):
        st=t.status()['state']
        if st in ('COMPLETED','FAILED'):
            print(f"🔔 {d} → {st}"); pend.pop(d)
    if pend: time.sleep(POLL_EVERY)
if pend: raise RuntimeError("Timeout tâches EE")

# ---------- 5. RÉCUPÉRATION & SFTP ------------------------------------------
q=f"'{FOLDER_ID}' in parents and trashed=false and name contains '.tif'"
files=drv_list(drv, q=q, fields='files(id,name)')

sent, errs=[],[]
tr=Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
sftp=SFTPClient.from_transport(tr)

for f in files:
    name,fid=f['name'],f['id']; loc=f"/tmp/{name}"
    drv_download(drv,fid,loc)
    try: sftp.put(loc, os.path.join(SFTP_DEST,name)); sent.append(name)
    except Exception as e: errs.append((name,str(e)))
    finally: os.remove(loc)

sftp.close(); tr.close()

# ---------- 6. MAIL ----------------------------------------------------------
body=[f"{len(tasks)} exports lancés.",
      f"{len(sent)} fichiers transférés :\n- "+"\n- ".join(sent)]
if errs: body.append(f"{len(errs)} erreur(s) SFTP :\n"+
                     "\n".join(f"{n}: {e}" for n,e in errs))

msg=MIMEMultipart(); msg['From']=SMTP_USER; msg['To']=','.join(EMAILS)
msg['Subject']="GEE → Drive → SFTP : "+("Succès" if not errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),'plain'))
with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USER,SMTP_PASS); s.send_message(msg)
print("✉️ Rapport envoyé.")