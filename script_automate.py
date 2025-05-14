#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP
• vidage « soft » d’un dossier partagé (ignore 403/404)
• polling réel des tâches EE + attente présence .tif
• transfert SFTP robuste + rapport mail
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------- PARAMÈTRES fixes -------------------------------------------------
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME      = 'Rahad-2';  TILE_ID = '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX      = 60
INDICES        = ['NDVI', 'EVI']
EXPORT_SCALE   = 10; EXPORT_CRS = 'EPSG:4326'
WAIT_TIME      = int(os.getenv('WAIT_TIME', 3600))     # timeout tâches EE
POLL_EVERY     = 30                                   # polling EE
FILE_TIMEOUT   = 600                                  # 10 min max apparition .tif

# ---------- SECRETS ----------------------------------------------------------
SA_KEY   = os.getenv('SA_KEY_PATH',     'sa-key.json')
SFTP_HOST= os.environ['SFTP_HOST'];     SFTP_PORT = int(os.getenv('SFTP_PORT',22))
SFTP_USER= os.environ['SFTP_USER'];     SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST= os.environ['SFTP_DEST_FOLDER']
SMTP_SRV = os.environ['SMTP_SERVER'];   SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USR = os.environ['SMTP_USER'];     SMTP_PWD  = os.environ['SMTP_PASS']
EMAILS   = os.environ['ALERT_EMAILS'].split(',')

# ---------- OUTILS Drive (retry réseau) -------------------------------------
RETRY_MAX = 6; BASE = 3
def _retry(fun, *a, **k):
    for i in range(1, RETRY_MAX+1):
        try: return fun(*a, **k)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if i == RETRY_MAX: raise
            d = BASE * 2**(i-1) * (0.5 + random.random()/2)
            print(f"🔄 retry {i}/{RETRY_MAX} dans {d:.1f}s – {e}")
            time.sleep(d)

def drv_list(svc, **kw): return _retry(lambda: svc.files().list(**kw).execute())['files']
def drv_del (svc, fid):
    try:
        _retry(lambda: svc.files().delete(fileId=fid).execute())
    except HttpError as e:
        if e.resp.status in (403,404):
            print(f"⚠️  skip {fid} (pas propriétaire)")
        else:
            raise

def drv_download(svc, fid, path):
    with open(path,'wb') as h:
        req = svc.files().get_media(fileId=fid)
        dl  = MediaIoBaseDownload(h, req)
        done = False
        while not done:
            _, done = _retry(dl.next_chunk)

# ---------- 1. INIT EE + Drive ----------------------------------------------
creds = service_account.Credentials.from_service_account_file(
    SA_KEY,
    scopes=[
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive'])
ee.Initialize(credentials=creds,project=creds.project_id); print("✅ EE OK")
drv = build('drive','v3',credentials=creds,cache_discovery=False); print("✅ Drive OK")

# ---------- 2. Dossier Drive (vidage sans suppression) ----------------------
q = (f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' "
     "and trashed=false")
res = drv_list(drv, q=q, fields='files(id)')
if not res:                            # dossier absent → on le crée
    meta={'name':DRIVE_FOLDER,'mimeType':'application/vnd.google-apps.folder'}
    FOLDER_ID = _retry(lambda: drv.files().create(body=meta,fields='id').execute())['id']
    print("• Dossier créé :", FOLDER_ID)
else:                                  # dossier présent → on le vide
    FOLDER_ID = res[0]['id']
    kids = drv_list(drv,
            q=f"'{FOLDER_ID}' in parents and trashed=false",
            fields='files(id)')
    for k in kids: drv_del(drv, k['id'])
    print(f"• Dossier vidé ({len(kids)} fichiers)")

# ---------- 3. Collection & Exports EE --------------------------------------
geom = (ee.FeatureCollection(ASSET_SITES)
          .filter(ee.Filter.eq('SITE',SITE_NAME)).first()).geometry()

ic = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterDate(DATE_START,DATE_END)
        .filterBounds(geom)
        .filterMetadata('CLOUDY_PIXEL_PERCENTAGE','less_than',CLOUD_MAX)
        .filter(ee.Filter.eq('MGRS_TILE',TILE_ID))
        .map(lambda img: img.updateMask(
            img.select('QA60').bitwiseAnd(1<<10).eq(0)
              .And(img.select('QA60').bitwiseAnd(1<<11).eq(0))).clip(geom))
        .map(lambda img: img.addBands([
            img.normalizedDifference(['B8','B4']).rename('NDVI'),
            ee.Image(2.5).multiply(
                img.select('B8').subtract(img.select('B4'))
                   .divide(img.select('B8').add(img.select('B4').multiply(6))
                                         .add(img.select('B2').multiply(-7.5))
                                         .add(1))).rename('EVI')])))

tasks=[]; lst=ic.toList(ic.size()); n=ic.size().getInfo()
for i in range(n):
    im = ee.Image(lst.get(i))
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for b in INDICES:
        dsc=f"{SITE_NAME}_{TILE_ID}_{date}_{b}"
        t = ee.batch.Export.image.toDrive(
                im.select(b).toFloat(), description=dsc,
                folder=DRIVE_FOLDER,fileNamePrefix=dsc,
                region=geom,scale=EXPORT_SCALE,crs=EXPORT_CRS,maxPixels=1e13)
        t.start(); tasks.append(t); print("🚀",dsc)

# ---------- 4. Polling EE ----------------------------------------------------
pend={t.status()['description']:t for t in tasks}; t0=time.time()
print(f"⏳ {len(pend)} tâche(s)…")
while pend and time.time()-t0<WAIT_TIME:
    for d,t in list(pend.items()):
        st=t.status()['state']
        if st in ('COMPLETED','FAILED'):
            print(f"🔔 {d} → {st}"); pend.pop(d)
    if pend: time.sleep(POLL_EVERY)
if pend: raise RuntimeError("⏰ timeout tâches EE")

# ---------- 5. Attente présence .tif ----------------------------------------
want=len(tasks); t0=time.time()
while True:
    files=drv_list(drv,
        q=f"'{FOLDER_ID}' in parents and trashed=false and name contains '.tif'",
        fields='files(id,name,size)')
    ready=[f for f in files if int(f.get('size','0'))>0]
    if len(ready)>=want: break
    if time.time()-t0>FILE_TIMEOUT:
        raise RuntimeError("Fichiers Drive encore incomplets après 10 min")
    print(f"⌛ {len(ready)}/{want} .tif prêts…"); time.sleep(15)

# ---------- 6. SFTP ----------------------------------------------------------
def sftp_mkdirs(sftp,path):            # création récursive
    cur=''
    for part in [p for p in path.split('/') if p]:
        cur+='/'+part
        try: sftp.listdir(cur)
        except IOError: sftp.mkdir(cur)

sent,errs=[],[]
tr=Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
sftp=SFTPClient.from_transport(tr); sftp_mkdirs(sftp,SFTP_DEST)

for f in ready:
    name,fid=f['name'],f['id']; tmp=f"/tmp/{name}"
    drv_download(drv,fid,tmp)
    try: sftp.put(tmp,f"{SFTP_DEST.rstrip('/')}/{name}"); sent.append(name)
    except Exception as e: errs.append((name,str(e)))
    finally: os.remove(tmp)

sftp.close(); tr.close()

# ---------- 7. Mail ----------------------------------------------------------
body=[f"{len(tasks)} exports lancés.",
      f"{len(sent)} fichiers transférés :\n- "+"\n- ".join(sent)]
if errs: body.append(f"{len(errs)} erreur(s) SFTP :\n"+
                     "\n".join(f"{n}: {e}" for n,e in errs))

msg=MIMEMultipart(); msg['From']=SMTP_USR; msg['To']=','.join(EMAILS)
msg['Subject']="GEE → Drive → SFTP : "+("Succès" if not errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),'plain'))
with smtplib.SMTP(SMTP_SRV,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USR,SMTP_PWD); s.send_message(msg)
print("✉️ Rapport envoyé")
