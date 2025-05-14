#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP (retry Drive, polling EE, rapport mail)
NE TOUCHE PAS aux parties déjà stables – correctif uniquement transfert
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ───── paramètres inchangés ─────────────────────────────────────────
DRIVE_FOLDER   = 'BRLi_Test_MEOSS'
ASSET_SITES    = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME, TILE_ID = 'Rahad-2', '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX = 60
INDICES   = ['NDVI', 'EVI']
SCALE, CRS = 10, 'EPSG:4326'
WAIT_TIME   = int(os.getenv('WAIT_TIME', 3600))
POLL_EE     = 30

SA_KEY   = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST = os.environ['SFTP_HOST'];  SFTP_PORT = int(os.getenv('SFTP_PORT',22))
SFTP_USER = os.environ['SFTP_USER'];  SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER = os.environ['SMTP_SERVER']; SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USER   = os.environ['SMTP_USER'];   SMTP_PASS = os.environ['SMTP_PASS']
EMAILS      = os.environ['ALERT_EMAILS'].split(',')

# ───── helper retry Drive (identique) ──────────────────────────────
RETRY_MAX, BASE_DELAY = 6, 3
def _retry(fn,*a,**kw):
    for i in range(1, RETRY_MAX+1):
        try: return fn(*a,**kw)
        except (ssl.SSLError,socket.error,HttpError) as e:
            if i==RETRY_MAX: raise
            d=BASE_DELAY*(2**(i-1))*(0.5+random.random()/2)
            print(f"🔄 retry {i}/{RETRY_MAX} dans {d:.1f}s → {e}")
            time.sleep(d)

def drv_list(svc,**kw):  return _retry(lambda: svc.files().list(**kw).execute())['files']
def drv_delete(svc,fid): _retry(lambda: svc.files().delete(fileId=fid).execute())

# ▲ 1. Correctif download Drive  (voir explication plus haut)
def drv_download(svc, fid, path):
    """télécharge fid → path, réessaie tant que Drive renvoie 0 octet."""
    for attempt in range(RETRY_MAX):
        with open(path, 'wb') as h:
            req = svc.files().get_media(fileId=fid)
            dl  = MediaIoBaseDownload(h, req)
            done = False
            while not done:
                _, done = _retry(dl.next_chunk)
        if os.path.getsize(path) > 0:     # OK
            return
        os.remove(path)                   # binaire vide → on retente
        wait = BASE_DELAY * (attempt + 1)
        print(f"⚠️  binaire encore vide, retry dans {wait}s…")
        time.sleep(wait)
    raise RuntimeError(f"Impossible de télécharger {fid}")

# ───── 1. EE & Drive init (inchangé) ───────────────────────────────
scopes=['https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive']
cred = service_account.Credentials.from_service_account_file(SA_KEY, scopes=scopes)
ee.Initialize(cred, project=cred.project_id);                  print("✅ EE OK")
drv = build('drive','v3',credentials=cred,cache_discovery=False);print("✅ Drive OK")

# ───── 2. dossier Drive (inchangé) ─────────────────────────────────
q=(f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' "
   "and 'root' in parents and trashed=false")
res=drv_list(drv,q=q,fields='files(id)')
if res:
    FID=res[0]['id']
    kids=drv_list(drv,q=f"'{FID}' in parents and trashed=false",fields='files(id)')
    for k in kids: drv_delete(drv,k['id'])
    print(f"• Dossier vidé ({len(kids)})")
else:
    meta={'name':DRIVE_FOLDER,'mimeType':'application/vnd.google-apps.folder'}
    FID=_retry(lambda: drv.files().create(body=meta,fields='id').execute())['id']
    print("• Dossier créé")

# ───── 3. exports EE (inchangé) ────────────────────────────────────
geom=(ee.FeatureCollection(ASSET_SITES)
        .filter(ee.Filter.eq('SITE',SITE_NAME)).first()).geometry()
col=(ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
     .filterDate(DATE_START,DATE_END)
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
                                   .add(1))).rename('EVI') ])))
tasks=[]
for im_id in col.aggregate_array('system:index').getInfo():
    im  = col.filter(ee.Filter.eq('system:index', im_id)).first()
    dat = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for b in INDICES:
        desc=f"{SITE_NAME}_{TILE_ID}_{dat}_{b}"
        t=ee.batch.Export.image.toDrive(im.select(b).toFloat(),
              description=desc, folder=DRIVE_FOLDER, fileNamePrefix=desc,
              region=geom, scale=SCALE, crs=CRS, maxPixels=1e13)
        t.start(); tasks.append(t); print("🚀",desc)

# ───── 4. polling (inchangé) ───────────────────────────────────────
pend={t.status()['description']:t for t in tasks}; st=time.time()
print(f"⏳ {len(pend)} tâche(s)…")
while pend and time.time()-st<WAIT_TIME:
    for d,t in list(pend.items()):
        if t.status()['state'] in ('COMPLETED','FAILED'):
            print(f"🔔 {d} → {t.status()['state']}"); pend.pop(d)
    if pend: time.sleep(POLL_EE)
if pend: raise RuntimeError("Timeout EE tâches")

# ───── 5. liste .tif (inchangé) ────────────────────────────────────
q=f"'{FID}' in parents and trashed=false and name contains '.tif'"
files=drv_list(drv,q=q,fields='files(id,name,size)')
while any(int(f.get('size','0'))==0 for f in files):
    time.sleep(5); files=drv_list(drv,q=q,fields='files(id,name,size)')

# ───── 6. download + SFTP (seul appel à drv_download modifié) ─────
sent,errs=[],[]
tr=Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
sftp=SFTPClient.from_transport(tr)
for f in files:
    name,fid=f['name'],f['id']; tmp=f"/tmp/{name}"
    drv_download(drv,fid,tmp)                      # ▲ correctif
    try:   sftp.put(tmp, os.path.join(SFTP_DEST,name)); sent.append(name)
    except Exception as e: errs.append((name,str(e)))
    finally: os.remove(tmp)
sftp.close(); tr.close()

# ───── 7. mail (inchangé) ──────────────────────────────────────────
body=[f"{len(tasks)} exports lancés.",
      f"{len(sent)} fichiers transférés :\n- " + ("\n- ".join(sent) or "—")]
if errs: body.append(f"{len(errs)} erreur(s) SFTP :\n"+
                     "\n".join(f"{n}: {e}" for n,e in errs))
msg=MIMEMultipart(); msg['From']=SMTP_USER; msg['To']=','.join(EMAILS)
msg['Subject']="GEE → Drive → SFTP : "+("Succès" if not errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),'plain'))
with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USER,SMTP_PASS); s.send_message(msg)
print("✉️ Rapport envoyé")
