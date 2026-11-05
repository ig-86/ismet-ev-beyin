"""İsmet Ev Beyin — Firebase (Firestore) posta kutusu <-> Home Assistant köprüsü.

Akış:
  Telefon `mesajlar` koleksiyonuna {kimden:'ben', durum:'yeni', metin, sesli} yazar.
  Beyin bunu dinler, komutsa (/kapi gibi) kendisi yapar, değilse HA'daki Claude
  sohbet ajanına sorar; cevabı {kimden:'ev'} olarak yazar, isteği 'tamam' işaretler.

Çalışma ortamı:
  HA eklentisi: /data/options.json + SUPERVISOR_TOKEN (otomatik).
  Bilgisayarda test: HA_URL, HA_TOKEN, SERVIS_HESABI_DOSYA ortam değişkenleri.
"""
import base64
import io
import json
import logging
import os
import queue
import threading
import time

import firebase_admin
import requests
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from PIL import Image

SURUM = "0.1.0"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("beyin")

# ---------------------------------------------------------------- ayarlar
def ayarlari_oku():
    secenek = {}
    if os.path.exists("/data/options.json"):
        secenek = json.load(open("/data/options.json", encoding="utf-8"))
    if os.environ.get("SERVIS_HESABI_DOSYA"):
        sa = json.load(open(os.environ["SERVIS_HESABI_DOSYA"], encoding="utf-8"))
    else:
        sa = json.loads(secenek.get("servis_hesabi") or "{}")
    if not sa:
        raise SystemExit("servis_hesabi ayarı boş — eklenti ayarlarına Firebase servis hesabı JSON'unu yapıştırın.")
    return {
        "sa": sa,
        "ajan": secenek.get("sohbet_ajani") or os.environ.get("SOHBET_AJANI", "conversation.claude_conversation"),
        "ha_url": os.environ.get("HA_URL", "http://supervisor/core"),
        "ha_token": os.environ.get("HA_TOKEN") or os.environ.get("SUPERVISOR_TOKEN"),
    }

AYAR = ayarlari_oku()
HA = requests.Session()
HA.headers["Authorization"] = "Bearer " + AYAR["ha_token"]

firebase_admin.initialize_app(credentials.Certificate(AYAR["sa"]))
db = firestore.client()
MESAJLAR = db.collection("mesajlar")
DURUM = db.collection("durum")

# ---------------------------------------------------------------- HA yardımcıları
def ha_durum(entity_id):
    r = HA.get(f"{AYAR['ha_url']}/api/states/{entity_id}", timeout=15)
    r.raise_for_status()
    return r.json()

def ha_servis(domain, servis, veri, cevapli=False):
    url = f"{AYAR['ha_url']}/api/services/{domain}/{servis}" + ("?return_response" if cevapli else "")
    r = HA.post(url, json=veri, timeout=90)
    r.raise_for_status()
    return r.json()

def kamera_resmi(entity_id, en=1280):
    """Kameradan anlık görüntü alır, Firestore'a sığacak şekilde küçültüp base64 döndürür."""
    r = HA.get(f"{AYAR['ha_url']}/api/camera_proxy/{entity_id}", timeout=30)
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content)).convert("RGB")
    if img.width > en:
        img = img.resize((en, round(img.height * en / img.width)))
    buf = io.BytesIO()
    for kalite in (75, 60, 45):
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=kalite, optimize=True)
        if buf.tell() < 600_000:  # Firestore belge sınırı 1 MB; base64 ile ~%33 büyür
            break
    return base64.b64encode(buf.getvalue()).decode()

# ---------------------------------------------------------------- Claude sohbeti
def claude_sor(metin):
    sohbet = DURUM.document("sohbet")
    kayit = sohbet.get().to_dict() or {}
    veri = {"text": metin, "agent_id": AYAR["ajan"], "language": "tr"}
    # 30 dakikadan eski sohbeti devam ettirme; boş conversation_id gönderme (hafıza bozulur)
    if kayit.get("conversation_id") and time.time() - kayit.get("zaman", 0) < 1800:
        veri["conversation_id"] = kayit["conversation_id"]
    cevap = ha_servis("conversation", "process", veri, cevapli=True)["service_response"]
    if cevap.get("conversation_id"):
        sohbet.set({"conversation_id": cevap["conversation_id"], "zaman": time.time()})
    return (cevap.get("response", {}).get("speech", {}).get("plain", {}).get("speech") or "").strip() or "(boş cevap)"

# ---------------------------------------------------------------- komutlar
def komut_kapi(_):
    return {"metin": f"🚪 Kapı kamerası ({time.strftime('%H:%M')})",
            "resim": kamera_resmi("camera.koridor_kapi_kamera_hd_stream")}

def komut_salon(_):
    if ha_durum("switch.salon_kamera_privacy")["state"] == "on":
        return {"metin": "🙈 Salon kamerası gizlilik modunda, görüntü alınamıyor."}
    return {"metin": f"🛋️ Salon kamerası ({time.strftime('%H:%M')})",
            "resim": kamera_resmi("camera.salon_kamera_hd_stream")}

def komut_isik(_):
    ent = "light.koridor_kapi_kamera_floodlight_timed"
    ha_servis("light", "toggle", {"entity_id": ent})
    time.sleep(2)
    acik = ha_durum(ent)["state"] == "on"
    return {"metin": f"🔦 Kapı kamerası ışığı {'AÇIK' if acik else 'KAPALI'}"}

def komut_yeni_sohbet(_):
    DURUM.document("sohbet").delete()
    return {"metin": "🧹 Yeni sohbet başladı, önceki konuşmayı unuttum."}

KOMUTLAR = {"/kapi": komut_kapi, "/salon": komut_salon, "/isik": komut_isik, "/yeni": komut_yeni_sohbet}

# ---------------------------------------------------------------- işleyici
is_kuyrugu = queue.Queue()

def isle(belge):
    veri = belge.to_dict() or {}
    metin = (veri.get("metin") or "").strip()
    log.info("mesaj %s: %s", belge.id, metin[:80])
    try:
        if not metin:
            cevap = {"metin": "Boş mesaj geldi."}
        elif metin.startswith("/"):
            f = KOMUTLAR.get(metin.split()[0].lower())
            cevap = f(metin) if f else {"metin": "Bu komutu bilmiyorum: " + metin}
        else:
            cevap = {"metin": claude_sor(metin)}
        durum = "tamam"
    except Exception as e:  # noqa: BLE001 — kullanıcıya her durumda cevap dönülmeli
        log.exception("hata")
        cevap = {"metin": f"⚠️ Bir sorun oldu: {e}"}
        durum = "hata"
    cevap.update({"kimden": "ev", "zaman": firestore.SERVER_TIMESTAMP, "durum": "tamam",
                  "sesli": bool(veri.get("sesli")), "cevap_verilen": belge.id})
    MESAJLAR.add(cevap)
    belge.reference.update({"durum": durum})

def isci():
    while True:
        belge = is_kuyrugu.get()
        try:
            # Aynı belge iki kez kuyruğa girmiş olabilir; hâlâ yeni mi kontrol et
            guncel = belge.reference.get()
            if (guncel.to_dict() or {}).get("durum") == "yeni":
                isle(guncel)
        except Exception:  # noqa: BLE001
            log.exception("işçi hatası")

def dinleyici(belgeler, degisiklikler, okuma_zamani):
    for d in degisiklikler:
        if d.type.name in ("ADDED", "MODIFIED"):
            is_kuyrugu.put(d.document)

def kalp_atisi():
    while True:
        try:
            DURUM.document("beyin").set({"son_gorulme": firestore.SERVER_TIMESTAMP, "surum": SURUM})
        except Exception:  # noqa: BLE001
            log.exception("kalp atışı yazılamadı")
        time.sleep(60)

def main():
    log.info("İsmet Ev Beyin %s başlıyor, sohbet ajanı: %s", SURUM, AYAR["ajan"])
    ha_durum("sun.sun")  # HA erişimini baştan doğrula
    threading.Thread(target=isci, daemon=True).start()
    threading.Thread(target=kalp_atisi, daemon=True).start()
    sorgu = MESAJLAR.where(filter=FieldFilter("kimden", "==", "ben")).where(filter=FieldFilter("durum", "==", "yeni"))
    izleyici = None
    while True:
        # Dinleyici koparsa yeniden kur
        if izleyici is None or getattr(izleyici, "is_active", True) is False:
            if izleyici is not None:
                log.warning("dinleyici koptu, yeniden kuruluyor")
            izleyici = sorgu.on_snapshot(dinleyici)
            log.info("mesajlar dinleniyor")
        time.sleep(30)

if __name__ == "__main__":
    main()
