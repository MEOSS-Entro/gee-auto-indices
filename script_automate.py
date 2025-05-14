#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP 100 % gratuit
• service‑account   • retry réseau   • polling EE & Drive
"""

import os, time, io, ssl, socket, random, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ───────────────────────── PARAMÈTRES MODIFIABLES ────────────────────────────
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE, TILE     = 'Rahad-2', '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX      = 60
INDICES        = ['NDVI', 'EVI']          # ajouter d’autres indices si besoin
SCALE, CRS     = 10, 'EPSG:4326'

WAIT_EE        = int(os.getenv('WAIT_TIME', 3600))   # timeout max EE
POLL_EE        = 30                                  # s
POLL_DRIVE     = 5                                   # s
RETRY_MAX      = 6                                   # appels Drive
BASE_DELAY     = 3                                   # s

# ─────────────────────────── VARIABLES SECRÈTES ──────────────────────────────
SA_KEY         = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST      = os.environ['SFTP_HOST'];  SFTP_PORT  = int(os.getenv('SFTP_PORT',22))
SFTP_USER      = os.environ['SFTP_USER'];  SFTP_PASS  = os.environ['SFTP_PASS']
SFTP_DEST      = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER    = os.environ['SMTP_SERVER']; SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USER      = os.environ['SMTP_USER'];   SMTP_PASS = os.environ['SMTP_PASS']
EMAILS         = os.environ['ALERT_EMAILS'].split(',')

# ─────────────────────────── OUTILS RÉSEAU ROBUSTES ──────────────────────────
def _retry(callable_, *a, **kw):
    for att in range(1, RETRY_MAX+1):
        try:
            return callable_(*a, **kw)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if att == RETRY_MAX:
                raise
            d = BASE_DELAY * (2**(att-1)) * (0.6+random.random()*0.8)
            print(f"🔄 Retry {att}/{RETRY_MAX} dans {d:.1f}s ({e})")
            time.sleep(d)

# ─────────────────────────── 1. INITIALISATION ───────────────────────────────
scopes = ['https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/earthengine',
          'https://www.googleapis.com/auth/drive']
cred   = service_account.Credentials.from_service_account_file(SA_KEY, scopes=scopes)
ee.Initialize(credentials=cred, project=cred.project_id);      print("✅ EE OK")
drv = build('drive','v3', credentials=cred, cache_discovery=False); print("✅ Drive API OK")

# ─────────────────────────── 2. DOSSIER DRIVE ────────────────────────────────
q=f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
files=_retry(lambda: drv.files().list(q=q,fields='files(id)').execute())['files']
if files:
    FID = files[0]['id']
    kids=_retry(lambda: drv.files().list(q=f"'{FID}' in parents and trashed=false",
                                        fields='files(id)').execute())['files']
    for k in kids: _retry(lambda: drv.files().delete(fileId=k['id']).execute())
    print(f"• Dossier vidé ({len(kids)})")
else:
    meta={'name':DRIVE_FOLDER,'mimeType':'application/vnd.google-apps.folder'}
    FID=_retry(lambda: drv.files().create(body=meta,fields='id').execute())['id']
    print("• Dossier créé")

# ─────────────────────────── 3. PRÉPARER COLLECTION EE ───────────────────────
geom=(ee.FeatureCollection(ASSET_SITES)
        .filter(ee.Filter.eq('SITE', SITE)).first()).geometry()

col=(ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
      .filterDate(DATE_START, DATE_END)
      .filterBounds(geom)
      .filterMetadata('CLOUDY_PIXEL_PERCENTAGE','less_than',CLOUD_MAX)
      .filter(ee.Filter.eq('MGRS_TILE',TILE_ID))
      .map(lambda im: im.updateMask(
            im.select('QA60').bitwiseAnd(1<<10).eq(0)
            .And(im.select('QA60').bitwiseAnd(1<<11).eq(0))).clip(geom))
      .map(lambda im: im.addBands([
            im.normalizedDifference(['B8','B4']).rename('NDVI'),
            ee.Image(2.5).multiply(
                im.select('B8').subtract(im.select('B4'))
                 .divide(im.select('B8').add(im.select('B4').multiply(6))
                                   .add(im.select('B2').multiply(-7.5))
                                   .add(1))).rename('EVI')
      ])))

# ─────────────────────────── 4. LANCER EXPORTS ───────────────────────────────
tasks=[]
for im in col.toList(col.size()).getInfo():
    ee_im = ee.Image(im)
    d=ee.Date(ee_im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for b in INDICES:
        name=f"{SITE}_{TILE_ID}_{d}_{b}"
        t=ee.batch.Export.image.toDrive(
            ee_im.select(b).toFloat(),description=name,
            folder=DRIVE_FOLDER,fileNamePrefix=name,
            region=geom,scale=SCALE,crs=CRS,maxPixels=1e13)
        t.start(); tasks.append(t); print("🚀",name)

# ─────────────────────────── 5. POLLING DES TÂCHES EE ────────────────────────
start=time.time(); pend={t.status()['description']:t for t in tasks}
print(f"⏳ {len(pend)} tâche(s) EE…")
while pend and time.time()-start<WAIT_EE:
    for d,t in list(pend.items()):
        st=t.status()['state']
        if st in ('COMPLETED','FAILED'):
            print(f"🔔 {d} → {st}"); pend.pop(d)
    if pend: time.sleep(POLL_EE)
if pend: raise RuntimeError("Timeout EE")
print("✅ Exports terminés.")

# ─────────────────────────── 6. ATTENTE DISPONIBILITÉ DRIVE ──────────────────
def wait_ready(file_id):
    while True:
        meta=_retry(lambda: drv.files().get(fileId=file_id,fields='size').execute())
        if int(meta.get('size','0'))>0: return
        time.sleep(POLL_DRIVE)

q=f"'{FID}' in parents and trashed=false and name contains '.tif'"
drive_files=_retry(lambda: drv.files().list(q=q,fields='files(id,name)').execute())['files']
for f in drive_files: wait_ready(f['id'])

# ─────────────────────────── 7. DOWNLOAD & SFTP ──────────────────────────────
sent, errs=[],[]
tr=Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
sftp=SFTPClient.from_transport(tr)

for f in drive_files:
    name=f['name']; fid=f['id']; loc=f"/tmp/{name}"
    try:
        # download
        with open(loc,'wb') as h:
            req=drv.files().get_media(fileId=fid)
            dl=MediaIoBaseDownload(h,req)
            done=False
            while not done: _,done=_retry(dl.next_chunk)
        # upload
        sftp.put(loc, os.path.join(SFTP_DEST,name)); sent.append(name)
    except Exception as e:
        errs.append((name,str(e)))
    finally:
        if os.path.exists(loc): os.remove(loc)

sftp.close(); tr.close()

# ─────────────────────────── 8. MAIL RAPPORT ────────────────────────────────
body=[f"{len(tasks)} exports lancés.",
      f"{len(sent)} fichiers transférés :\n- "+"\n- ".join(sent) or "—"]
if errs:
    body.append(f"{len(errs)} erreur(s) SFTP :\n"+"\n".join(f"{n}: {e}" for n,e in errs))

msg=MIMEMultipart(); msg['From']=SMTP_USER; msg['To']=','.join(EMAILS)
msg['Subject']="GEE → Drive → SFTP : "+("Succès" if not errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),'plain'))
with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USER,SMTP_PASS); s.send_message(msg)
print("✉️ Rapport envoyé.")