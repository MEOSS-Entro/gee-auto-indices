# -*- coding: utf-8 -*-
"""
Created on Tue May 13 11:20:20 2025

@author: abezd
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE → Drive → SFTP, gratuit
- Vide le dossier Drive partagé
- Exporte, poll jusqu'à COMPLETED/FAILED
- Télécharge via PyDrive2, envoie SFTP
- Rapport mail
"""

import os, time, ee, smtplib, traceback, io
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.http import MediaIoBaseDownload

# --- CONSTANTES ---
DRIVE_FOLDER = 'BRLi_Test_MEOSS'
ASSET_SITES  = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME    = 'Rahad-2'
TILE_ID      = '36PWA'
DATE_START, DATE_END = '2025-04-07', '2025-04-10'
CLOUD_MAX    = 60
INDICES      = ['NDVI','EVI']
EXPORT_SCALE = 10
EXPORT_CRS   = 'EPSG:4326'
WAIT_TIME    = int(os.getenv('WAIT_TIME', 3600))
POLL_EVERY   = 30

# --- Secrets depuis GitHub Actions ---
with open('sa-key.json','w') as f:
    f.write(os.environ['GEE_SA_KEY'])
EE_KEY = 'sa-key.json'

SFTP_HOST, SFTP_PORT  = os.environ['SFTP_HOST'], int(os.getenv('SFTP_PORT',22))
SFTP_USER, SFTP_PASS  = os.environ['SFTP_USER'], os.environ['SFTP_PASS']
SFTP_DEST             = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER           = os.environ['SMTP_SERVER']
SMTP_PORT             = int(os.environ['SMTP_PORT'])
SMTP_USER             = os.environ['SMTP_USER']
SMTP_PASS             = os.environ['SMTP_PASS']
EMAILS                = os.environ['ALERT_EMAILS'].split(',')

# --- Init Earth Engine (SA) ---
ee.Initialize(ServiceAccountCredentials.from_json_keyfile_name(
    EE_KEY, scopes=['https://www.googleapis.com/auth/earthengine']))
print("✅ EE initialisé.")

# --- Init PyDrive2 (OAuth utilisateur) ---
gauth = GoogleAuth()
gauth.settings['client_config_file'] = 'client_secrets.json'
gauth.LoadCredentialsFile('token.json')
if not gauth.credentials or gauth.access_token_expired:
    gauth.LocalWebserverAuth()
    gauth.SaveCredentialsFile('token.json')
drive = GoogleDrive(gauth)
print("✅ PyDrive2 prêt.")

# --- 1. Nettoyer / créer dossier ---
fld_q = f"title='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
folders = drive.ListFile({'q':fld_q}).GetList()
if folders:
    fld = folders[0]
    for f in drive.ListFile({'q':f"'{fld['id']}' in parents and trashed=false"}).GetList():
        f.Delete()
    print("• Dossier vidé.")
else:
    fld = drive.CreateFile({'title':DRIVE_FOLDER,'mimeType':'application/vnd.google-apps.folder'})
    fld.Upload()
    print("• Dossier créé.")
FOLDER_ID = fld['id']          # pour PyDrive2
FOLDER_NAME = DRIVE_FOLDER     # pour Export.image.toDrive

# --- 2. Lancer exports, stocker Task objets ---
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
        .And(img.select('QA60').bitwiseAnd(1<<11).eq(0))
      ).clip(geom))
      .map(lambda img: img.addBands([
          img.normalizedDifference(['B8','B4']).rename('NDVI'),
          ee.Image(2.5).multiply(
             img.select('B8').subtract(img.select('B4'))
               .divide(img.select('B8').add(img.select('B4').multiply(6))
                       .add(img.select('B2').multiply(-7.5))
                       .add(1))
          ).rename('EVI')
      ])))
tasks = []
for img in ic.toList(ic.size()).getInfo():
    im = ee.Image(img)
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for b in INDICES:
        desc = f"{SITE_NAME}_{TILE_ID}_{date}_{b}"
        t = ee.batch.Export.image.toDrive(im.select(b).toFloat(),
            description=desc, folder=FOLDER_NAME, fileNamePrefix=desc,
            region=geom, scale=EXPORT_SCALE, crs=EXPORT_CRS, maxPixels=1e13)
        t.start(); tasks.append(t); print("🚀", desc)

# --- 3. Polling ---
start = time.time()
pending = {t.status()['description']:t for t in tasks}
while pending and time.time()-start < WAIT_TIME:
    for desc,t in list(pending.items()):
        st = t.status()['state']
        if st in ('COMPLETED','FAILED'):
            print(f"🔔 {desc} → {st}")
            pending.pop(desc)
    if pending: time.sleep(POLL_EVERY)
if pending:
    raise RuntimeError(f"Tâches non terminées: {list(pending)}")
print("✅ Tous exports terminés.")

# --- 4. Télécharger & SFTP ---
query = f"'{FOLDER_ID}' in parents and trashed=false and title contains '.tif'"
files = drive.ListFile({'q':query}).GetList()
ok, err = [], []
tr = Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)
for f in files:
    name = f['title']; f.GetContentFile(name)
    try:
        sftp.put(name, os.path.join(SFTP_DEST,name)); ok.append(name)
    except Exception as e:
        err.append((name,str(e)))
    finally:
        os.remove(name)
sftp.close(); tr.close()

# --- 5. Rapport mail ---
body = [f"{len(tasks)} exports lancés.",
        f"{len(ok)} fichiers transférés:\n- "+"\n- ".join(ok)]
if err: body.append(f"{len(err)} erreurs SFTP:\n"+"".join(f"{n}:{e}\n" for n,e in err))
def mail(subj, txt):
    msg = MIMEMultipart(); msg['From']=SMTP_USER; msg['To']=','.join(EMAILS); msg['Subject']=subj
    msg.attach(MIMEText(txt,'plain'))
    with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
        s.starttls(); s.login(SMTP_USER,SMTP_PASS); s.send_message(msg)
try:
    mail("Succès GEE→Drive→SFTP", "\n\n".join(body))
except Exception:
    print("⚠️  Envoi mail échoué")

