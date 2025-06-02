#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE  ➜  Drive  ➜  SFTP  (multi-sites, 7 indices)
— Vidage « soft » des dossiers Drive par site
— Composites 10-jours (LAI, NDVI, EVI, NDRE, MSAVI, SIWSI, NMDI)
— Polling robustes (EE & Drive), transfert SFTP, rapport mail
"""

import os, time, io, random, ssl, socket, ee, smtplib, traceback
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─────────── SITES & PARAMÈTRES GÉNÉRAUX ────────────────────────────────────
SITE_IDS = [
    'projects/gee-flow-meoss/assets/kibimba',
    'projects/gee-flow-meoss/assets/renk'
]
INDICES        = ['NDVI', 'EVI', 'LAI', 'NDRE', 'MSAVI', 'SIWSI', 'NMDI']
CLOUD_PROB_THRESHOLD = 40
EXPORT_SCALE   = 10
EXPORT_CRS     = 'EPSG:4326'
START_DATE     = ee.Date('2025-03-25')
END_DATE       = ee.Date('2025-05-25')
WAIT_TIME      = int(os.getenv('WAIT_TIME', 3600))      # timeout EE (par site)
POLL_EVERY     = 30                                     # polling EE
FILE_TIMEOUT   = 600                                    # 10 min .tif
empty_img = ee.Image.constant([-32767]*len(INDICES)).rename(INDICES).updateMask(ee.Image.constant(0))

# ─────────── SECRETS ENV ─────────────────────────────────────────────────────
SA_KEY_PATH = os.getenv('SA_KEY_PATH', 'sa-key.json')
SFTP_HOST   = os.environ['SFTP_HOST'];   SFTP_PORT = int(os.getenv('SFTP_PORT', 22))
SFTP_USER   = os.environ['SFTP_USER'];   SFTP_PASS = os.environ['SFTP_PASS']
SFTP_DEST   = os.environ['SFTP_DEST_FOLDER']
SMTP_SRV    = os.environ['SMTP_SERVER']; SMTP_PORT = int(os.environ['SMTP_PORT'])
SMTP_USR    = os.environ['SMTP_USER'];   SMTP_PWD  = os.environ['SMTP_PASS']
EMAILS      = os.environ['ALERT_EMAILS'].split(',')

# ─────────── OUTILS Drive (réessaies réseau) ────────────────────────────────
RETRY_MAX = 6; BASE = 3
def _retry(fun, *a, **k):
    for i in range(1, RETRY_MAX+1):
        try:
            return fun(*a, **k)
        except (ssl.SSLError, socket.error, HttpError) as e:
            if i == RETRY_MAX:
                raise
            d = BASE * 2**(i-1) * (0.5 + random.random()/2)
            print(f" retry {i}/{RETRY_MAX} dans {d:.1f}s – {e}")
            time.sleep(d)

def drv_list(svc, **kw): return _retry(lambda: svc.files().list(**kw).execute())['files']
def drv_del (svc, fid):
    try:
        _retry(lambda: svc.files().delete(fileId=fid).execute())
    except HttpError as e:
        if e.resp.status in (403,404):
            print(f"  skip {fid} (pas propriétaire)")
        else:
            raise
def drv_download(svc, fid, path):
    with open(path,'wb') as h:
        req = svc.files().get_media(fileId=fid)
        dl  = MediaIoBaseDownload(h, req)
        done = False
        while not done:
            _, done = _retry(dl.next_chunk)

# ─────────── 1. Authentification EE + Drive ─────────────────────────────────
creds = service_account.Credentials.from_service_account_file(
    SA_KEY_PATH,
    scopes=[
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive'])
ee.Initialize(credentials=creds, project=creds.project_id); print(" EE OK")
drv = build('drive','v3',credentials=creds,cache_discovery=False); print(" Drive OK")

# ─────────── 2. Fonctions EE (indices + composites) ─────────────────────────
def mask_cloud_shadow(img):
    prob = ee.Image(img.get('cloud_prob')).select('probability')
    qa = img.select('QA60')
    mask = prob.lt(CLOUD_PROB_THRESHOLD).And(
        qa.bitwiseAnd(1 << 10).eq(0)).And(
        qa.bitwiseAnd(1 << 11).eq(0))
    return img.updateMask(mask).copyProperties(img, ['system:time_start'])

def add_all_indices(img):
    b = {f'B{i}': img.select(f'B{i}').divide(1e4).toFloat()
         for i in [2,3,4,5,6,8,11,12]}
    ndvi  = b['B8'].subtract(b['B4']).divide(b['B8'].add(b['B4'])).rename('NDVI')
    evi   = ee.Image(2.5).multiply(
                b['B8'].subtract(b['B4']).divide(
                    b['B8'].add(b['B4'].multiply(6))
                           .add(b['B2'].multiply(-7.5)).add(1))).rename('EVI')
    lai   = evi.multiply(3.618).subtract(0.118).rename('LAI')
    ndre  = b['B8'].subtract(b['B5']).divide(b['B8'].add(b['B5'])).rename('NDRE')
    msavi = b['B8'].multiply(2).add(1).subtract(
                (b['B8'].multiply(2).add(1)).pow(2)
                .subtract(b['B8'].subtract(b['B4']).multiply(8)).sqrt()
            ).divide(2).rename('MSAVI')
    siwsi = b['B11'].subtract(b['B8']).divide(b['B11'].add(b['B8'])).rename('SIWSI')
    nmdi  = b['B8'].subtract(b['B11'].subtract(b['B12'])).divide(
                b['B8'].add(b['B11'].subtract(b['B12']))).rename('NMDI')
    return img.addBands([ndvi, evi, lai, ndre, msavi, siwsi, nmdi])

def dekad_composite(start, end, geom):
    s2sr   = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
               .filterBounds(geom).filterDate(start, end)
    s2prob = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
               .filterBounds(geom).filterDate(start, end)
    joined = ee.Join.saveFirst('cloud_prob').apply(
                s2sr, s2prob, ee.Filter.equals('system:index','system:index'))
    col = (ee.ImageCollection(joined)
            .map(mask_cloud_shadow)
            .map(add_all_indices)
            .map(lambda im: im.addBands(
                ee.Image(im.get('cloud_prob')).select('probability')
                  .multiply(-1).add(100).rename('cloud_score'))))
    return ee.Image(ee.Algorithms.If(
        col.size().gt(0),
        col.qualityMosaic('cloud_score').select(INDICES),
        empty_img)).set('system:time_start', start.millis())

# ─────────── 3. Fonctions SFTP ──────────────────────────────────────────────
def sftp_mkdirs(sftp, path):
    cur = ''
    for part in [p for p in path.split('/') if p]:
        cur += '/' + part
        try:
            sftp.listdir(cur)
        except IOError:
            sftp.mkdir(cur)

# ─────────── 4. Boucle sites ────────────────────────────────────────────────
global_sent, global_errs, nb_tasks_tot = [], [], 0

for site_id in SITE_IDS:
    AOI_FC   = ee.FeatureCollection(site_id)
    AOI      = ee.Feature(AOI_FC.first())
    AOI_GEOM = AOI.geometry()
    SITE_NAME= AOI.get('Nom').getInfo() or site_id.split('/')[-1]

    print(f"\n {SITE_NAME}: création/vidage du dossier Drive")
    # Dossier Drive (niveau racine, nommé comme le site)
    q = (f"name='{SITE_NAME}' and mimeType='application/vnd.google-apps.folder' "
         "and trashed=false")
    res = drv_list(drv, q=q, fields='files(id)')
    if not res:
        meta = {'name': SITE_NAME,
                'mimeType': 'application/vnd.google-apps.folder'}
        FOLDER_ID = _retry(lambda: drv.files()
                   .create(body=meta, fields='id').execute())['id']
        print("• Dossier créé :", FOLDER_ID)
    else:
        FOLDER_ID = res[0]['id']
        kids = drv_list(drv, q=f"'{FOLDER_ID}' in parents and trashed=false",
                        fields='files(id)')
        for k in kids:
            drv_del(drv, k['id'])
        print(f"• Dossier vidé ({len(kids)} fichiers)")

    # — Exports EE —
    step = 10            # avancer de 10 jours
    period = 30          # fenêtré sur 30 jours (3 dekades)
    n_days = END_DATE.difference(START_DATE, 'day').getInfo()
    tasks = []
    for offset in range(0, n_days - period + 1, step):
        start = START_DATE.advance(offset, 'day')
        d1, d2 = start, start.advance(10,'day')
        d3, end = d2.advance(10,'day'), d2.advance(20,'day')
        mid = start.advance(15,'day')
        dek1 = dekad_composite(d1, d2, AOI_GEOM)
        dek2 = dekad_composite(d2, d3, AOI_GEOM)
        dek3 = dekad_composite(d3, end, AOI_GEOM)
        interp = dek1.add(dek3).divide(2)
        filled = dek2.where(dek2.mask().Not(), interp)
        bounded = (filled.select(['NDVI','EVI','NDRE','MSAVI','SIWSI','NMDI'])
                   .clamp(-1,1)).addBands(filled.select('LAI').clamp(-1,7))
        scaled  = bounded.multiply(10000).round().toInt16()
        final_img = scaled.clip(AOI_GEOM).unmask(-32768).toInt16()
        date_str = mid.format('YYYYMMdd').getInfo()

        for band in INDICES:
            filename = f"{SITE_NAME}_{band}_{date_str}"
            t = ee.batch.Export.image.toDrive(
                    image=final_img.select(band),
                    description=filename,
                    folder=SITE_NAME,
                    fileNamePrefix=filename,
                    region=AOI_GEOM,
                    scale=EXPORT_SCALE,
                    crs=EXPORT_CRS,
                    maxPixels=1e13)
            t.start(); tasks.append(t); print("f", filename)
    nb_tasks_tot += len(tasks)

    # — Polling EE —
    pend = {t.status()['description']: t for t in tasks}; t0 = time.time()
    print(f" {len(pend)} tâche(s) EE… (timeout {WAIT_TIME//60} min)")
    while pend and time.time() - t0 < WAIT_TIME:
        for d, t in list(pend.items()):
            st = t.status()['state']
            if st in ('COMPLETED', 'FAILED'):
                print(f" {d} → {st}"); pend.pop(d)
        if pend:
            time.sleep(POLL_EVERY)
    if pend:
        raise RuntimeError(f"{SITE_NAME}: timeout tâches EE")

    # — Attente .tif —
    want = len(tasks); t0 = time.time()
    while True:
        files = drv_list(drv,
            q=f"'{FOLDER_ID}' in parents and trashed=false and name contains '.tif'",
            fields='files(id,name,size)')
        ready = [f for f in files if int(f.get('size', '0')) > 0]
        if len(ready) >= want:
            break
        if time.time() - t0 > FILE_TIMEOUT:
            raise RuntimeError(f"{SITE_NAME}: .tif incomplets après 10 min")
        print(f" {len(ready)}/{want} .tif prêts…"); time.sleep(15)

    # — Transfert SFTP —
    sent, errs = [], []
    tr = Transport((SFTP_HOST, SFTP_PORT))
    tr.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = SFTPClient.from_transport(tr)
    remote_site = f"{SFTP_DEST.rstrip('/')}/{SITE_NAME}"
    sftp_mkdirs(sftp, remote_site)

    for f in ready:
        name, fid = f['name'], f['id']; tmp = f"/tmp/{name}"
        drv_download(drv, fid, tmp)
        try:
            sftp.put(tmp, f"{remote_site}/{name}"); sent.append(f"{SITE_NAME}/{name}")
        except Exception as e:
            errs.append((f"{SITE_NAME}/{name}", str(e)))
        finally:
            os.remove(tmp)
    sftp.close(); tr.close()

    global_sent.extend(sent); global_errs.extend(errs)
    print(f" {SITE_NAME}: {len(sent)} fichiers transférés")

# ─────────── 5. Mail récapitulatif ──────────────────────────────────────────
body = [
    f"{nb_tasks_tot} exports lancés sur {len(SITE_IDS)} sites.",
    f"{len(global_sent)} fichiers transférés :\n- " + "\n- ".join(global_sent)
]
if global_errs:
    body.append(f"{len(global_errs)} erreur(s) SFTP :\n" +
                "\n".join(f"{n}: {e}" for n, e in global_errs))

msg = MIMEMultipart()
msg['From'] = SMTP_USR
msg['To']   = ','.join(EMAILS)
msg['Subject'] = "GEE → Drive → SFTP : " + \
    ("Succès" if not global_errs else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body), 'plain'))
with smtplib.SMTP(SMTP_SRV, SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USR, SMTP_PWD); s.send_message(msg)
print("  Rapport envoyé")
