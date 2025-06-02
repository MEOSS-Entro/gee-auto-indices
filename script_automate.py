#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EE → Drive → SFTP (Automatisation multi-sites / multi-indices)
- Extraction dekadique glissante de 7 indices pour 5 sites
- Résultats enregistrés dans un dossier Google Drive par site
- Transfert des .tif de chaque site vers son répertoire homologue sur le SFTP
- Rapport e-mail final
"""

import os, time, io, random, ssl, socket, ee, smtplib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------- Paramètres généraux ----------
SITE_IDS = [
    'projects/gee-flow-meoss/assets/koga',
    'projects/gee-flow-meoss/assets/renk',
    'projects/gee-flow-meoss/assets/kanyonyomba',
    'projects/gee-flow-meoss/assets/kibimba',
    'projects/gee-flow-meoss/assets/rahad'
]
INDICES = ['NDVI','EVI','LAI','NDRE','MSAVI','SIWSI','NMDI']
EXPORT_SCALE, EXPORT_CRS = 10, 'EPSG:4326'
CLOUD_PROB_THRESHOLD = 40
START_DATE = ee.Date('2025-03-25')
END_DATE   = ee.Date('2025-05-25')
WAIT_TIME  = int(os.getenv('WAIT_TIME', 3600))
FILE_TIMEOUT = 600

# ---------- Secrets / env ----------
SA_KEY   = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST= os.environ['SFTP_HOST']; SFTP_PORT = int(os.environ['SFTP_PORT'])
SFTP_USER= os.environ['SFTP_USER']; SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST= os.environ['SFTP_DEST_FOLDER']
SMTP_SRV = os.environ['SMTP_SERVER']; SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USR = os.environ['SMTP_USER'];   SMTP_PWD  = os.environ['SMTP_PASS']
EMAILS   = os.environ['ALERT_EMAILS'].split(',')

# ---------- Drive utils ----------
RETRY_MAX, BASE = 6, 3

def _retry(fun, *a, **k):
    for i in range(1, RETRY_MAX+1):
        try: return fun(*a, **k)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if i == RETRY_MAX: raise
            time.sleep(BASE * 2**(i-1) * (0.5 + random.random()/2))

def drv_list(svc, **kw): return _retry(lambda: svc.files().list(**kw).execute())['files']
def drv_del(svc, fid):
    try: _retry(lambda: svc.files().delete(fileId=fid).execute())
    except HttpError as e:
        if e.resp.status not in (403, 404): raise

def drv_download(svc, fid, path):
    with open(path,'wb') as h:
        req = svc.files().get_media(fileId=fid)
        dl = MediaIoBaseDownload(h, req)
        done = False
        while not done: _, done = _retry(dl.next_chunk)

# ---------- Init Earth Engine ----------
cred = service_account.Credentials.from_service_account_file(SA_KEY, scopes=[
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/earthengine',
    'https://www.googleapis.com/auth/drive'])
ee.Initialize(cred, project=cred.project_id)
drv = build('drive', 'v3', credentials=cred, cache_discovery=False)

# ---------- Traitement principal ----------
all_sent, all_errs = [], []

def sftp_mkdirs(sftp, path):
    cur = ''
    for part in [p for p in path.split('/') if p]:
        cur += '/' + part
        try: sftp.listdir(cur)
        except IOError: sftp.mkdir(cur)

for site_id in SITE_IDS:
    AOI_FC = ee.FeatureCollection(site_id)
    AOI = ee.Feature(AOI_FC.first())
    AOI_GEOM = AOI.geometry()
    SITE_NAME = AOI.get('Nom').getInfo() or site_id.split('/')[-1]

    # Nettoyage Drive
    q = (f"name='{SITE_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false")
    res = drv_list(drv, q=q, fields='files(id)')
    if not res:
        meta={'name':SITE_NAME,'mimeType':'application/vnd.google-apps.folder'}
        FID = _retry(lambda: drv.files().create(body=meta,fields='id').execute())['id']
    else:
        FID = res[0]['id']
        kids = drv_list(drv, q=f"'{FID}' in parents and trashed=false", fields='files(id)')
        for k in kids: drv_del(drv,k['id'])

    tasks=[]; step=10; period=30; n_days=END_DATE.difference(START_DATE,'day').getInfo()
    for offset in range(0, n_days - period + 1, step):
        d1 = START_DATE.advance(offset,'day')
        d2 = d1.advance(10,'day')
        d3 = d2.advance(10,'day')
        end = d3.advance(10,'day')
        mid = d1.advance(15,'day')

        def dekad(start,end):
            s2 = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED').filterBounds(AOI_GEOM).filterDate(start,end)
            s2c= ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY').filterBounds(AOI_GEOM).filterDate(start,end)
            joined = ee.Join.saveFirst('cloud_prob').apply(s2,s2c,ee.Filter.equals('system:index','system:index'))
            def process(img):
                img=ee.Image(img); cp=ee.Image(img.get('cloud_prob')).select('probability')
                mask = cp.lt(CLOUD_PROB_THRESHOLD).And(img.select('QA60').bitwiseAnd(1<<10).eq(0)).And(img.select('QA60').bitwiseAnd(1<<11).eq(0))
                return img.updateMask(mask).copyProperties(img,['system:time_start'])
            def indices(img):
                b = {f'B{i}': img.select(f'B{i}').divide(1e4) for i in [2,3,4,5,6,8,11,12]}
                ndvi  = b['B8'].subtract(b['B4']).divide(b['B8'].add(b['B4'])).rename('NDVI')
                evi   = ee.Image(2.5).multiply(b['B8'].subtract(b['B4']).divide(b['B8'].add(b['B4'].multiply(6)).add(b['B2'].multiply(-7.5)).add(1))).rename('EVI')
                lai   = evi.multiply(3.618).subtract(0.118).rename('LAI')
                ndre  = b['B8'].subtract(b['B5']).divide(b['B8'].add(b['B5'])).rename('NDRE')
                msavi = b['B8'].multiply(2).add(1).subtract((b['B8'].multiply(2).add(1)).pow(2).subtract(b['B8'].subtract(b['B4']).multiply(8)).sqrt()).divide(2).rename('MSAVI')
                siwsi = b['B11'].subtract(b['B8']).divide(b['B11'].add(b['B8'])).rename('SIWSI')
                nmdi  = b['B8'].subtract(b['B11'].subtract(b['B12'])).divide(b['B8'].add(b['B11'].subtract(b['B12']))).rename('NMDI')
                return img.addBands([ndvi,evi,lai,ndre,msavi,siwsi,nmdi])
            col = ee.ImageCollection(joined).map(process).map(indices)
            score = col.map(lambda img: ee.Image(img.get('cloud_prob')).multiply(-1).add(100).rename('cloud_score'))
            return ee.Image(col.qualityMosaic('cloud_score').select(INDICES))

        dek1 = dekad(d1,d2)
        dek2 = dekad(d2,d3)
        dek3 = dekad(d3,end)
        interp = dek1.add(dek3).divide(2)
        filled = dek2.where(dek2.mask().Not(), interp)
        bounded = filled.select(['NDVI','EVI','NDRE','MSAVI','SIWSI','NMDI']).clamp(-1, 1)
        bounded = bounded.addBands(filled.select('LAI').clamp(-1, 7))
        final = bounded.multiply(10000).round().toInt16().clip(AOI_GEOM).unmask(-32768)

        date_str = mid.format('YYYYMMdd').getInfo()
        for band in INDICES:
            filename = f"{SITE_NAME}_{band}_{date_str}"
            t = ee.batch.Export.image.toDrive(
                image=final.select(band), description=filename,
                folder=SITE_NAME, fileNamePrefix=filename,
                region=AOI_GEOM, scale=EXPORT_SCALE, crs=EXPORT_CRS, maxPixels=1e13)
            t.start(); tasks.append(t)

    # Polling
    pend={t.status()['description']:t for t in tasks}; t0=time.time()
    while pend and time.time()-t0<WAIT_TIME:
        for d,t in list(pend.items()):
            st=t.status()['state']
            if st in ('COMPLETED','FAILED'):
                pend.pop(d)
        if pend: time.sleep(30)
    if pend: raise RuntimeError(f"Timeout GEE ({SITE_NAME})")

    # Attente des fichiers Drive
    want=len(tasks); t0=time.time()
    while True:
        files=drv_list(drv,q=f"'{FID}' in parents and trashed=false and name contains '.tif'",fields='files(id,name,size)')
        ready=[f for f in files if int(f.get('size','0'))>0]
        if len(ready)>=want: break
        if time.time()-t0>FILE_TIMEOUT:
            raise RuntimeError(f"Timeout fichiers Drive ({SITE_NAME})")
        time.sleep(15)

    # Transfert SFTP
    tr = Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
    sftp = SFTPClient.from_transport(tr); sftp_mkdirs(sftp,f"{SFTP_DEST}/{SITE_NAME}")
    for f in ready:
        name,fid = f['name'],f['id']; tmp = f"/tmp/{name}"
        drv_download(drv,fid,tmp)
        try:
            sftp.put(tmp,f"{SFTP_DEST.rstrip('/')}/{SITE_NAME}/{name}")
            all_sent.append(name)
        except Exception as e:
            all_errs.append((name,str(e)))
        finally:
            os.remove(tmp)
    sftp.close(); tr.close()

# ---------- Mail final ------------------------------------------------------
body = [f"{len(all_sent)} fichiers transférés :", "- " + "\n- ".join(all_sent)]
if all_errs:
    body.append(f"{len(all_errs)} erreur(s) :\n" + "\n".join(f"{n}: {e}" for n,e in all_errs))
msg = MIMEMultipart(); msg['From']=SMTP_USR; msg['To']=','.join(EMAILS)
msg['Subject']="GEE Indices : " + ("Succès" if not all_errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),'plain'))
with smtplib.SMTP(SMTP_SRV,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USR,SMTP_PWD); s.send_message(msg)
print("✉️ Rapport envoyé")