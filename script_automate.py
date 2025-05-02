# -*- coding: utf-8 -*-
"""
Created on Fri Apr 25 17:17:31 2025

@author: Ezzeddine ABBESSI
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatisation bi-hebdomadaire via GitHub Actions
– Earth Engine → Drive → SFTP
– Nettoyage de dossier Drive
– Rapport par e-mail
"""

import os
import time
import ee
import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
from googleapiclient.discovery import build
from paramiko import Transport, SFTPClient

# -----------------------------------------------------------------------------
# 1) Chargement des variables d’environnement (à configurer en GitHub Secrets)
# -----------------------------------------------------------------------------
# Clé JSON du compte de service (Earth Engine + Drive)
SA_KEY_PATH       = os.getenv('SA_KEY_PATH', 'sa-key.json')

# GEE / Drive
ASSET_SITES       = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME         = 'Rahad-2'
TILE_ID           = '36PWA'
DATE_START        = '2025-04-07'
DATE_END          = '2025-04-10'
CLOUD_MAX         = 60
DRIVE_FOLDER_NAME = 'BRLi_Test_MEOSS'       # dossier unique Drive
EXPORT_SCALE      = 10
EXPORT_CRS        = 'EPSG:4326'
INDICES           = ['NDVI','EVI']         # ou liste complète

# Timing
WAIT_TIME         = int(os.getenv('WAIT_TIME', 10*60))  # en secondes

# SFTP (depuis GitHub Secrets)
SFTP_HOST         = os.environ['SFTP_HOST']
SFTP_PORT         = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER         = os.environ['SFTP_USER']
SFTP_PASS         = os.environ['SFTP_PASS']
SFTP_DEST_FOLDER  = os.environ['SFTP_DEST_FOLDER']

# Mail SMTP (Gmail SMTP ou autre)
SMTP_SERVER       = os.environ['SMTP_SERVER']
SMTP_PORT         = int(os.environ.get('SMTP_PORT', 587))
SMTP_USER         = os.environ['SMTP_USER']
SMTP_PASS         = os.environ['SMTP_PASS']
EMAIL_FROM        = SMTP_USER
EMAIL_TO          = os.environ['ALERT_EMAILS'].split(',')  # "a@x.com,b@y.com"

# -----------------------------------------------------------------------------
# 2) Initialisations “headless”
# -----------------------------------------------------------------------------
def init_earthengine():
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials=creds)
    print("✅ Earth Engine initialisé (service account).")

def init_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    svc = build('drive', 'v3', credentials=creds, cache_discovery=False)
    print("✅ Google Drive API initialisée.")
    return svc

# -----------------------------------------------------------------------------
# 3) Gestion du dossier Drive
# -----------------------------------------------------------------------------
def find_or_create_folder(drive_svc, name):
    # Cherche un dossier racine portant ce nom
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    resp = drive_svc.files().list(q=q, fields='files(id,name)').execute()
    files = resp.get('files', [])
    if files:
        folder_id = files[0]['id']
        # Vide le dossier
        q = f"'{folder_id}' in parents and trashed=false"
        kids = drive_svc.files().list(q=q, fields='files(id)').execute().get('files', [])
        for k in kids:
            drive_svc.files().delete(fileId=k['id']).execute()
        print(f"• Dossier existant vidé ({len(kids)} fichiers).")
    else:
        # Crée un dossier neuf
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
        folder = drive_svc.files().create(body=meta, fields='id').execute()
        folder_id = folder['id']
        print("• Dossier créé :", name)
    return folder_id

# -----------------------------------------------------------------------------
# 4) Lancement des exports Earth Engine → Drive
# -----------------------------------------------------------------------------
def compute_and_export(folder_id):
    fc   = ee.FeatureCollection(ASSET_SITES)
    feat = fc.filter(ee.Filter.eq('SITE', SITE_NAME)).first()
    geom = feat.geometry()
    coll = (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
          .filterDate(DATE_START, DATE_END)
          .filterBounds(geom)
          .filterMetadata('CLOUDY_PIXEL_PERCENTAGE','less_than',CLOUD_MAX)
          .filter(ee.Filter.eq('MGRS_TILE', TILE_ID))
          .map(lambda img: img.updateMask(
               img.select('QA60').bitwiseAnd(1<<10).eq(0)
             .And(img.select('QA60').bitwiseAnd(1<<11).eq(0))
             ).clip(geom))
          .map(lambda img: img
            .addBands([
              img.normalizedDifference(['B8','B4']).rename('NDVI'),
              ee.Image(2.5).multiply(
                img.select('B8').subtract(img.select('B4'))
                   .divide(
                     img.select('B8').add(img.select('B4').multiply(6))
                        .add(img.select('B2').multiply(-7.5))
                        .add(1)
                   )
              ).rename('EVI')
            ])
          )
    )
    ids = coll.aggregate_array('system:index').getInfo()
    launched = []
    for idx in ids:
        img     = coll.filter(ee.Filter.eq('system:index', idx)).first()
        dateStr = ee.Date(img.get('system:time_start')).format('yyyyMMdd').getInfo()
        for band in INDICES:
            name = f"{SITE_NAME}_{TILE_ID}_{dateStr}_{band}"
            task = ee.batch.Export.image.toDrive(
                image          = img.select(band).toFloat(),
                description    = name,
                folder         = folder_id,
                fileNamePrefix = name,
                region         = geom,
                scale          = EXPORT_SCALE,
                crs            = EXPORT_CRS,
                maxPixels      = 1e13
            )
            task.start()
            launched.append(name)
            print("🚀 Export lancé :", name)
    return launched

# -----------------------------------------------------------------------------
# 5) Attente
# -----------------------------------------------------------------------------
def wait_exports(seconds):
    print(f"⏳ Attente de {seconds//60} min pour complétion...")
    time.sleep(seconds)

# -----------------------------------------------------------------------------
# 6) Téléchargement & SFTP
# -----------------------------------------------------------------------------
def list_drive_tifs(drive_svc, folder_id):
    files, token = [], None
    q = f"'{folder_id}' in parents and trashed=false and name contains '.tif'"
    while True:
        resp = drive_svc.files().list(q=q,
                                      fields='nextPageToken,files(id,name)',
                                      pageToken=token).execute()
        files.extend(resp.get('files', []))
        token = resp.get('nextPageToken')
        if not token: break
    return files

def sftp_transfer(files):
    transport = Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = SFTPClient.from_transport(transport)
    sent, errors = [], []
    for f in files:
        nm, fid = f['name'], f['id']
        local = f"/tmp/{nm}"
        # Download
        with open(local, 'wb') as fh:
            drive_svc.files().get_media(fileId=fid).execute_to(fh)
        # Upload
        try:
            sftp.put(local, os.path.join(SFTP_DEST_FOLDER, nm))
            sent.append(nm)
        except Exception as e:
            errors.append((nm,str(e)))
        finally:
            os.remove(local)
    sftp.close()
    transport.close()
    return sent, errors

# -----------------------------------------------------------------------------
# 7) Envoi de mail de rapport
# -----------------------------------------------------------------------------
def send_report(subject, body):
    msg = MIMEMultipart()
    msg['From']    = EMAIL_FROM
    msg['To']      = ','.join(EMAIL_TO)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    print("✉️  Rapport envoyé.")

# -----------------------------------------------------------------------------
# 8) Orchestration principale
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    report_lines = []
    try:
        init_earthengine()
        drive_svc = init_drive_service()
        folder_id = find_or_create_folder(drive_svc, DRIVE_FOLDER_NAME)

        exports = compute_and_export(folder_id)
        report_lines.append(f"{len(exports)} export(s) lancés:\n- " + "\n- ".join(exports))

        wait_exports(WAIT_TIME)

        tifs = list_drive_tifs(drive_svc, folder_id)
        report_lines.append(f"{len(tifs)} TIF prêts sur Drive.")

        transferred, errors = sftp_transfer(tifs)
        report_lines.append(f"{len(transferred)} fichier(s) transféré(s):\n- " + "\n- ".join(transferred))
        if errors:
            report_lines.append(f"{len(errors)} erreur(s) SFTP:\n" +
                                "\n".join(f"{n}: {e}" for n,e in errors))

        # on ne supprime PAS sur Drive ici
        subject = "GEE → SFTP : Succès automatisé"
        send_report(subject, "\n\n".join(report_lines))

    except Exception:
        tb = traceback.format_exc()
        send_report("GEE → SFTP : Erreur critique", tb)
        raise
