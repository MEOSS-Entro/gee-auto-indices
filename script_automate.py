#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE → Drive utilisateur → SFTP
- Vide le dossier partagé
- Exporte avec EE (service account)
- Attend la fin réelle (polling)
- Télécharge via PyDrive2
- Envoie sur SFTP
- Envoie un rapport e-mail
"""

import os, time, ee, smtplib, traceback, json
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from paramiko import Transport, SFTPClient

# -----------------------------------------------------------------------------
# VARIABLES
# -----------------------------------------------------------------------------

DRIVE_FOLDER = 'BRLi_Test_MEOSS'
ASSET_SITES = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME = 'Rahad-2'
TILE_ID = '36PWA'
DATE_START = '2025-04-07'
DATE_END = '2025-04-10'
CLOUD_MAX = 60
INDICES = ['NDVI', 'EVI']
EXPORT_SCALE = 10
EXPORT_CRS = 'EPSG:4326'
WAIT_TIME = int(os.getenv('WAIT_TIME', 3600))
POLL_INTERVAL = 30

# -----------------------------------------------------------------------------
# SECRETS (via GitHub Actions)
# -----------------------------------------------------------------------------

with open('sa-key.json', 'w') as f:
    f.write(os.environ['GEE_SA_KEY'])

EE_KEY = 'sa-key.json'
SFTP_HOST = os.environ['SFTP_HOST']
SFTP_PORT = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER = os.environ['SFTP_USER']
SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST = os.environ['SFTP_DEST_FOLDER']
SMTP_SERVER = os.environ['SMTP_SERVER']
SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USER = os.environ['SMTP_USER']
SMTP_PASS = os.environ['SMTP_PASS']
EMAILS = os.environ['ALERT_EMAILS'].split(',')

# -----------------------------------------------------------------------------
# INIT
# -----------------------------------------------------------------------------

ee.Initialize(ServiceAccountCredentials.from_json_keyfile_name(
    EE_KEY, scopes=['https://www.googleapis.com/auth/earthengine']))
print("✅ EE initialisé.")

gauth = GoogleAuth()
gauth.settings['client_config_file'] = 'client_secrets.json'
gauth.LoadCredentialsFile('token.json')
if not gauth.credentials or gauth.access_token_expired:
    gauth.LocalWebserverAuth()
    gauth.SaveCredentialsFile('token.json')
drive = GoogleDrive(gauth)
print("✅ PyDrive2 connecté.")

# -----------------------------------------------------------------------------
# 1. Nettoyer / créer le dossier partagé
# -----------------------------------------------------------------------------

query = f"title='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
folders = drive.ListFile({'q': query}).GetList()
if folders:
    folder = folders[0]
    for f in drive.ListFile({'q': f"'{folder['id']}' in parents and trashed=false"}).GetList():
        f.Delete()
    print("• Dossier vidé.")
else:
    folder = drive.CreateFile({'title': DRIVE_FOLDER, 'mimeType': 'application/vnd.google-apps.folder'})
    folder.Upload()
    print("• Dossier créé.")
FOLDER_ID = folder['id']

# -----------------------------------------------------------------------------
# 2. Lancer les exports Earth Engine
# -----------------------------------------------------------------------------

fc = ee.FeatureCollection(ASSET_SITES)
geom = fc.filter(ee.Filter.eq('SITE', SITE_NAME)).first().geometry()

coll = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterDate(DATE_START, DATE_END)
        .filterBounds(geom)
        .filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'less_than', CLOUD_MAX)
        .filter(ee.Filter.eq('MGRS_TILE', TILE_ID))
        .map(lambda img: img.updateMask(
            img.select('QA60').bitwiseAnd(1 << 10).eq(0)
            .And(img.select('QA60').bitwiseAnd(1 << 11).eq(0))
        ).clip(geom))
        .map(lambda img: img.addBands([
            img.normalizedDifference(['B8', 'B4']).rename('NDVI'),
            ee.Image(2.5).multiply(
                img.select('B8').subtract(img.select('B4'))
                .divide(
                    img.select('B8').add(img.select('B4').multiply(6))
                    .add(img.select('B2').multiply(-7.5))
                    .add(1))
            ).rename('EVI')
        ])))

exports = []
for img in coll.toList(coll.size()).getInfo():
    im = ee.Image(img['id'])
    date = ee.Date(im.get('system:time_start')).format('yyyyMMdd').getInfo()
    for band in INDICES:
        name = f"{SITE_NAME}_{TILE_ID}_{date}_{band}"
        task = ee.batch.Export.image.toDrive(
            image=im.select(band).toFloat(),
            description=name,
            folder=DRIVE_FOLDER,
            fileNamePrefix=name,
            region=geom,
            scale=EXPORT_SCALE,
            crs=EXPORT_CRS,
            maxPixels=1e13
        )
        task.start()
        exports.append((name, task))
        print("🚀 Export lancé :", name)

# -----------------------------------------------------------------------------
# 3. Polling des tâches
# -----------------------------------------------------------------------------

start = time.time()
while exports and time.time() - start < WAIT_TIME:
    for name, task in list(exports):
        state = task.status()['state']
        if state in ('COMPLETED', 'FAILED'):
            print(f"🔔 {name} → {state}")
            exports.remove((name, task))
    if exports:
        time.sleep(POLL_INTERVAL)
if exports:
    raise RuntimeError("Tâches non terminées à temps :", [n for n, _ in exports])
print("✅ Tous exports terminés.")

# -----------------------------------------------------------------------------
# 4. Télécharger et transférer
# -----------------------------------------------------------------------------

files = drive.ListFile({'q': f"'{FOLDER_ID}' in parents and trashed=false and title contains '.tif'"}).GetList()
sent, errors = [], []
tr = Transport((SFTP_HOST, SFTP_PORT))
tr.connect(username=SFTP_USER, password=SFTP_PASS)
sftp = SFTPClient.from_transport(tr)

for f in files:
    name = f['title']
    f.GetContentFile(name)
    try:
        sftp.put(name, os.path.join(SFTP_DEST, name))
        sent.append(name)
    except Exception as e:
        errors.append((name, str(e)))
    finally:
        os.remove(name)

sftp.close()
tr.close()

# -----------------------------------------------------------------------------
# 5. Envoyer rapport par email
# -----------------------------------------------------------------------------

def send_report(subject, body):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = SMTP_USER, ','.join(EMAILS), subject
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

report = [
    f"{len(files)} fichiers trouvés dans le dossier.",
    f"{len(sent)} transférés :\n- " + "\n- ".join(sent)
]
if errors:
    report.append(f"{len(errors)} erreurs :\n" + "\n".join(f"{n}: {e}" for n, e in errors))
try:
    send_report("GEE → SFTP : Rapport complet", "\n\n".join(report))
    print("✉️ Rapport envoyé.")
except Exception as e:
    print("⚠️ Erreur d'envoi e-mail :", str(e))
