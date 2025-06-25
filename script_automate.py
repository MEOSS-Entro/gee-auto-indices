#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GEE  ->  Drive  ->  SFTP  (multi-sites, 7 indices)

– composites 10 j (NDVI, EVI, LAI, NDRE, MSAVI, SIWSI, NMDI)
– START_DATE = aujourd’hui – 30 j ; END_DATE = aujourd’hui (UTC)
– dossiers Drive vidés avant export, .tif conservés
– NoData = –32768 à l’extérieur du polygone du site (dans l’emprise raster)
"""

import os, time, random, ssl, socket
from datetime import datetime, timedelta, timezone
import ee, smtplib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from paramiko import Transport, SFTPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─────────── Secrets & env ──────────────────────────────────────────────────
SA_KEY_PATH = os.getenv("SA_KEY_PATH", "sa-key.json")

SFTP_HOST = os.environ["SFTP_HOST"];  SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.environ["SFTP_USER"];  SFTP_PASS = os.environ["SFTP_PASS"]
SFTP_DEST = "/Data/PROD"

SMTP_SRV  = os.environ["SMTP_SERVER"]; SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USR  = os.environ["SMTP_USER"];   SMTP_PWD  = os.environ["SMTP_PASS"]
EMAILS    = os.environ["ALERT_EMAILS"].split(",")

WAIT_TIME     = int(os.getenv("WAIT_TIME",     14_400))
PER_TASK_WAIT = int(os.getenv("PER_TASK_WAIT",     900))
FILE_TIMEOUT  = int(os.getenv("FILE_TIMEOUT",    1_800))
POLL_EVERY    = 30

# ─────────── Auth EE + Drive ────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_file(
    SA_KEY_PATH,
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/earthengine",
        "https://www.googleapis.com/auth/drive",
    ],
)
ee.Initialize(credentials=creds, project=creds.project_id)
drv = build("drive", "v3", credentials=creds, cache_discovery=False)

# ─────────── Paramètres fixes ───────────────────────────────────────────────
SITE_IDS = [
    "projects/gee-flow-meoss/assets/koga",
    "projects/gee-flow-meoss/assets/kibimba",
    "projects/gee-flow-meoss/assets/kanyonyomba",
    "projects/gee-flow-meoss/assets/renk",
    "projects/gee-flow-meoss/assets/rahad",
]

INDICES = ["NDVI", "EVI", "LAI", "NDRE", "MSAVI", "SIWSI", "NMDI"]
CLOUD_PROB_THRESHOLD = 40
EXPORT_SCALE, EXPORT_CRS = 10, "EPSG:4326"

today = datetime.now(timezone.utc).date()
END_DATE   = ee.Date(str(today))
START_DATE = ee.Date(str(today - timedelta(days=30)))

empty_img = ee.Image.constant([-32767]*len(INDICES)).rename(INDICES).updateMask(ee.Image.constant(0))

# ─────────── Outils Drive (retry) ───────────────────────────────────────────
def _retry(fun,*a,**k):
    for i in range(1,7):
        try: return fun(*a,**k)
        except (ssl.SSLError,socket.error,HttpError):
            if i==6: raise
            time.sleep(3*2**(i-1)*random.uniform(0.5,1.0))

def drv_list(**kw): return _retry(lambda: drv.files().list(**kw).execute())["files"]
def drv_del(fid):     _retry(lambda: drv.files().delete(fileId=fid).execute())
def drv_download(fid,path):
    with open(path,"wb") as h:
        req = drv.files().get_media(fileId=fid)
        dl  = MediaIoBaseDownload(h,req)
        done=False
        while not done: _,done = _retry(dl.next_chunk)

# ─────────── Fonctions EE ───────────────────────────────────────────────────
def mask_cloud_shadow(img):
    prob = ee.Image(img.get("cloud_prob")).select("probability")
    qa   = img.select("QA60")
    mask = prob.lt(CLOUD_PROB_THRESHOLD)\
           .And(qa.bitwiseAnd(1<<10).eq(0))\
           .And(qa.bitwiseAnd(1<<11).eq(0))
    return img.updateMask(mask).copyProperties(img, ["system:time_start"])

def add_all_indices(img):
    b = {f"B{i}": img.select(f"B{i}").divide(1e4).toFloat() for i in [2,3,4,5,6,8,11,12]}
    ndvi = b["B8"].subtract(b["B4"]).divide(b["B8"].add(b["B4"])).rename("NDVI")
    evi  = ee.Image(2.5).multiply(
             b["B8"].subtract(b["B4"]).divide(
               b["B8"].add(b["B4"].multiply(6)).add(b["B2"].multiply(-7.5)).add(1))).rename("EVI")
    lai  = evi.multiply(3.618).subtract(0.118).rename("LAI")
    ndre = b["B8"].subtract(b["B5"]).divide(b["B8"].add(b["B5"])).rename("NDRE")
    msavi= b["B8"].multiply(2).add(1).subtract(
             (b["B8"].multiply(2).add(1)).pow(2)
             .subtract(b["B8"].subtract(b["B4"]).multiply(8)).sqrt()).divide(2).rename("MSAVI")
    siwsi= b["B11"].subtract(b["B8"]).divide(b["B11"].add(b["B8"])).rename("SIWSI")
    nmdi = b["B8"].subtract(b["B11"].subtract(b["B12"]))\
             .divide(b["B8"].add(b["B11"].subtract(b["B12"]))).rename("NMDI")
    return img.addBands([ndvi,evi,lai,ndre,msavi,siwsi,nmdi])

def dekad_composite(start,end,geom):
    s2   = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterBounds(geom).filterDate(start,end)
    prob = ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY").filterBounds(geom).filterDate(start,end)
    joined = ee.Join.saveFirst("cloud_prob").apply(
               s2, prob, ee.Filter.equals(leftField="system:index", rightField="system:index"))
    col = (ee.ImageCollection(joined)
            .map(mask_cloud_shadow).map(add_all_indices)
            .map(lambda im: im.addBands(
                ee.Image(im.get("cloud_prob")).select("probability")
                  .multiply(-1).add(100).rename("cloud_score"))))
    return ee.Image(
        ee.Algorithms.If(col.size().gt(0), col.qualityMosaic("cloud_score").select(INDICES), empty_img)
    ).set("system:time_start", start.millis())

# ─────────── SFTP helper ────────────────────────────────────────────────────
def sftp_mkdirs(sftp,path):
    cur=""
    for part in [p for p in path.split("/") if p]:
        cur+="/"+part
        try: sftp.listdir(cur)
        except IOError: sftp.mkdir(cur)

# ─────────── Boucle sites ───────────────────────────────────────────────────
sent_all, errs_all, tot_tasks = [], [], 0

for site_id in SITE_IDS:
    fc = ee.FeatureCollection(site_id); feat = ee.Feature(fc.first())
    geom = feat.geometry()
    site = feat.get("Nom").getInfo() or site_id.split("/")[-1]
    print(f"\n=== {site} ===")

    # -------- Dossier Drive --------
    res = drv_list(q=f"name='{site}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                   fields="files(id)")
    if res: folder_id = res[0]["id"]
    else:
        meta={"name":site,"mimeType":"application/vnd.google-apps.folder"}
        folder_id = _retry(lambda: drv.files().create(body=meta, fields="id").execute())["id"]
    for f in drv_list(q=f"'{folder_id}' in parents and trashed=false", fields="files(id)"):
        drv_del(f["id"])

    # -------- Exports EE --------
    tasks=[]; step,period = 10,30
    n_days = END_DATE.difference(START_DATE,'day').getInfo()

    for off in range(0, n_days-period+1, step):
        d1 = START_DATE.advance(off,'day')
        d2,d3,d4 = d1.advance(10,'day'), d1.advance(20,'day'), d1.advance(30,'day')
        mid = d1.advance(15,'day')

        dek1,dek2,dek3 = dekad_composite(d1,d2,geom), dekad_composite(d2,d3,geom), dekad_composite(d3,d4,geom)
        filled = dek2.where(dek2.mask().Not(), dek1.add(dek3).divide(2))

        bounded = filled.select(["NDVI","EVI","NDRE","MSAVI","SIWSI","NMDI"]).clamp(-1,1)\
                  .addBands(filled.select("LAI").clamp(-1,7))

        scaled = bounded.multiply(10000).round().toInt16()

        # ---------- MODIF : masque NoData -32768 hors polygone ----------
        site_mask = ee.Image.constant(1).clip(geom)                # 1 sur le site
        img = scaled.updateMask(site_mask).unmask(-32768).toInt16()
        # ----------------------------------------------------------------

        date_str = mid.format("YYYYMMdd").getInfo()
        for band in INDICES:
            fn = f"{site}_{band}_{date_str}"
            task = ee.batch.Export.image.toDrive(
                image   = img.select(band),
                description = fn,
                folder  = site,
                fileNamePrefix = fn,
                region  = img.geometry(),      # emprise complète
                scale   = EXPORT_SCALE,
                crs     = EXPORT_CRS,
                maxPixels = 1e13)
            task.start(); tasks.append(task); print("Export lancé :", fn)

    tot_tasks += len(tasks)

    # -------- Polling EE --------
    site_wait = max(WAIT_TIME, PER_TASK_WAIT*len(tasks))
    pending = {t.id:t for t in tasks}; t0=time.time()
    while pending and time.time()-t0 < site_wait:
        for tid,t in list(pending.items()):
            if t.status()["state"] in ("COMPLETED","FAILED","CANCELLED","CANCEL_REQUESTED"):
                pending.pop(tid)
        if pending: time.sleep(POLL_EVERY)
    if pending: raise RuntimeError(f"{site} : timeout EE ({len(pending)} restantes)")

    # -------- Attente .tif --------
    want=len(tasks); t0=time.time()
    while True:
        files = drv_list(q=f"'{folder_id}' in parents and trashed=false and name contains '.tif'",
                         fields="files(id,name,size)")
        ready=[f for f in files if int(f.get("size","0"))>0]
        if len(ready)>=want: break
        if time.time()-t0 > FILE_TIMEOUT:
            raise RuntimeError(f"{site} : .tif incomplets après {FILE_TIMEOUT//60} min")
        time.sleep(15)

    # -------- Transfert SFTP --------
    sent,errs=[],[]
    tr=Transport((SFTP_HOST,SFTP_PORT)); tr.connect(username=SFTP_USER,password=SFTP_PASS)
    sftp=SFTPClient.from_transport(tr)
    remote=f"{SFTP_DEST.rstrip('/')}/{site}"; sftp_mkdirs(sftp,remote)

    for f in ready:
        name,fid=f["name"],f["id"]; tmp=f"/tmp/{name}"
        drv_download(fid,tmp)
        try: sftp.put(tmp,f"{remote}/{name}"); sent.append(f"{site}/{name}")
        except Exception as e: errs.append((f"{site}/{name}",str(e)))
        finally: os.remove(tmp)
    sftp.close(); tr.close()

    sent_all.extend(sent); errs_all.extend(errs)
    print(f"{site} : {len(sent)} fichiers transférés (Drive conservé)")

# ─────────── Mail de synthèse ───────────────────────────────────────────────
body = [
    f"{tot_tasks} exports lancés sur {len(SITE_IDS)} sites.",
    f"{len(sent_all)} fichiers transférés :\n- " + "\n- ".join(sent_all)
]
if errs_all:
    body.append(f"{len(errs_all)} erreur(s) SFTP :\n" +
                "\n".join(f"{n} : {e}" for n,e in errs_all))

msg = MIMEMultipart(); msg["From"]=SMTP_USR; msg["To"]=",".join(EMAILS)
msg["Subject"]="GEE -> Drive -> SFTP : " + ("Succès" if not errs_all else "Succès (avec erreurs)")
msg.attach(MIMEText("\n\n".join(body),"plain"))

with smtplib.SMTP(SMTP_SRV,SMTP_PORT) as s:
    s.starttls(); s.login(SMTP_USR,SMTP_PWD); s.send_message(msg)

print("Rapport mail envoyé")