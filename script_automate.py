#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatisation bi-hebdomadaire via GitHub Actions
– Earth Engine → Drive → SFTP
– Nettoyage de dossier Drive
– Rapport par e-mail
"""

import os, time, ee, smtplib, traceback, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from paramiko import Transport, SFTPClient
from googleapiclient.http import MediaIoBaseDownload
import io

# -------------------------------------------------------------------
# 1) Variables d’environnement (GitHub Secrets / Workflow env)
# -------------------------------------------------------------------
SA_KEY_PATH       = os.getenv('SA_KEY_PATH', 'sa-key.json')
# GEE & Drive
ASSET_SITES       = 'projects/gee-flow-meoss/assets/Sites_BRLi_2Km'
SITE_NAME         = 'Rahad-2'
TILE_ID           = '36PWA'
DATE_START        = '2025-04-07'
DATE_END          = '2025-04-10'
CLOUD_MAX         = 60
DRIVE_FOLDER_NAME = 'BRLi_Test_MEOSS'
EXPORT_SCALE      = 10
EXPORT_CRS        = 'EPSG:4326'
INDICES           = ['NDVI','EVI']
WAIT_TIME         = int(os.getenv('WAIT_TIME', 10*60))
# SFTP
SFTP_HOST         = os.environ['SFTP_HOST']
SFTP_PORT         = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER         = os.environ['SFTP_USER']
SFTP_PASS         = os.environ['SFTP_PASS']
SFTP_DEST_FOLDER  = os.environ['SFTP_DEST_FOLDER']
# SMTP
SMTP_SERVER       = os.environ['SMTP_SERVER']
SMTP_PORT         = int(os.environ.get('SMTP_PORT', 587))
SMTP_USER         = os.environ['SMTP_USER']
SMTP_PASS         = os.environ['SMTP_PASS']
EMAIL_FROM        = SMTP_USER
EMAIL_TO          = os.environ['ALERT_EMAILS'].split(',')

# -------------------------------------------------------------------
# 2) Init headless Earth Engine & Drive
# -------------------------------------------------------------------
def init_earthengine():
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials=creds)
    print("✅ Earth Engine initialisé.")

def init_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    svc = build('drive', 'v3', credentials=creds, cache_discovery=False)
    print("✅ Google Drive API initialisée.")
    return svc

# -------------------------------------------------------------------
# 3) Gestion du dossier Drive
# -------------------------------------------------------------------
def find_or_create_folder(drive_svc, name):
    q = (
      f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
      "and 'root' in parents and trashed=false"
    )
    resp = drive_svc.files().list(q=q, fields='files(id)').execute()
    files = resp.get('files', [])
    if files:
        folder_id = files[0]['id']
        # Vide
        q2 = f"'{folder_id}' in parents and trashed=false"
        kids = drive_svc.files().list(q=q2, fields='files(id)').execute().get('files', [])
        for k in kids:
            drive_svc.files().delete(fileId=k['id']).execute()
        print(f"• Dossier existant vidé ({len(kids)} fichiers).")
    else:
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
        folder = drive_svc.files().create(body=meta, fields='id').execute()
        folder_id = folder['id']
        print("• Dossier créé :", name)
    return folder_id

# -------------------------------------------------------------------
# 4) Exports GEE → Drive
# -------------------------------------------------------------------
def compute_and_export(folder_id):
    fc = ee.FeatureCollection(ASSET_SITES)
    feat = fc.filter(ee.Filter.eq('SITE', SITE_NAME)).first()
    geom = feat.geometry()
    coll = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
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
        ]))
    )
    ids = coll.aggregate_array('system:index').getInfo()
    launched = []
    for idx in ids:
        img = coll.filter(ee.Filter.eq('system:index', idx)).first()
        dateStr = ee.Date(img.get('system:time_start')).format('yyyyMMdd').getInfo()
        for band in INDICES:
            name = f"{SITE_NAME}_{TILE_ID}_{dateStr}_{band}"
            ee.batch.Export.image.toDrive(
                image=img.select(band).toFloat(),
                description=name,
                folder=DRIVE_FOLDER_NAME,
                fileNamePrefix=name,
                region=geom,
                scale=EXPORT_SCALE,
                crs=EXPORT_CRS,
                maxPixels=1e13
            ).start()
            launched.append(name)
            print("🚀 Export lancé:", name)
    return launched

# -------------------------------------------------------------------
# 5) Attente
# -------------------------------------------------------------------
def wait_exports(seconds):
    print(f"⏳ Attente de {seconds//60} min…")
    time.sleep(seconds)

# -------------------------------------------------------------------
# 6) Listing & SFTP avec retries SSL
# -------------------------------------------------------------------
def list_drive_tifs(drive_svc, folder_id):
    files, token = [], None
    q = f"'{folder_id}' in parents and trashed=false and name contains '.tif'"
    while True:
        for attempt in range(1, 4):
            try:
                resp = drive_svc.files().list(
                    q=q, pageSize=100,
                    fields='nextPageToken,files(id,name)',
                    pageToken=token
                ).execute()
                break
            except (HttpError, ssl.SSLError) as e:
                print(f"⚠️ list_drive_tifs erreur {attempt}/3: {e}")
                time.sleep(5 * attempt)
        else:
            raise RuntimeError("list_drive_tifs échoué après 3 tentatives")
        batch = resp.get('files', [])
        files.extend(batch)
        token = resp.get('nextPageToken')
        if not token:
            break
    return files

def sftp_transfer(drive_svc, files):
    transport = Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = SFTPClient.from_transport(transport)
    sent, errors = [], []
    for f in files:
        nm, fid = f['name'], f['id']
        local = f"/tmp/{nm}"
        fh = io.FileIO(local, 'wb')
        downloader = MediaIoBaseDownload(fh, drive_svc.files().get_media(fileId=fid))
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        try:
            sftp.put(local, os.path.join(SFTP_DEST_FOLDER, nm))
            sent.append(nm)
        except Exception as e:
            errors.append((nm, str(e)))
        finally:
            os.remove(local)
    sftp.close()
    transport.close()
    return sent, errors

# -------------------------------------------------------------------
# 7) Rapport mail
# -------------------------------------------------------------------
def send_report(subject, body):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = EMAIL_FROM, ','.join(EMAIL_TO), subject
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    print("✉️ Rapport envoyé.")

# -------------------------------------------------------------------
# 8) Main orchestration
# -------------------------------------------------------------------
if __name__ == '__main__':
    report = []
    try:
        init_earthengine()
        drive_svc = init_drive_service()
        fid = find_or_create_folder(drive_svc, DRIVE_FOLDER_NAME)

        exports = compute_and_export(fid)
        report.append(f"{len(exports)} exports lancés:\n- " + "\n- ".join(exports))

        wait_exports(WAIT_TIME)

        tifs = list_drive_tifs(drive_svc, fid)
        report.append(f"{len(tifs)} TIF trouvés sur Drive.")

        sent, errs = sftp_transfer(drive_svc, tifs)
        report.append(f"{len(sent)} transférés:\n- " + "\n- ".join(sent))
        if errs:
            report.append(f"{len(errs)} erreurs SFTP:\n" +
                          "\n".join(f"{n}: {e}" for n,e in errs))

        send_report("GEE → SFTP : Succès", "\n\n".join(report))

    except Exception as ex:
        tb = traceback.format_exc()
        send_report("GEE → SFTP : Erreur critique", tb)
        raise
