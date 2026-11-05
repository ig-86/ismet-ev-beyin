"""İsmet Ev Beyin — Firebase (Firestore + FCM) <-> Home Assistant köprüsü.

Akış:
  Telefon `mesajlar` koleksiyonuna {kimden:'ben', durum:'yeni', metin, sesli} yazar.
  Beyin bunu dinler, komutsa (/kapi gibi) kendisi yapar, değilse HA'daki Claude
  sohbet ajanına sorar; cevabı {kimden:'ev'} olarak yazar, isteği 'tamam' işaretler.

  HA otomasyonları `ismet_ev_bildirim` olayı ateşleyerek telefona bildirim gönderir:
    event_data: baslik, metin, resim_dosya (/config/... yolu, ops.), kamera (ops.),
                butonlar ('kapi' gibi hazır set adı, ops.), kanal ('kapi' | 'genel')

Çalışma ortamı:
  HA eklentisi: /data/options.json + SUPERVISOR_TOKEN (otomatik), /homeassistant = HA config (salt okunur).
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
import websocket
from firebase_admin import credentials, firestore, messaging
from google.cloud.firestore_v1.base_query import FieldFilter
from PIL import Image

SURUM = "0.2.0"
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
    ha_url = os.environ.get("HA_URL", "http://supervisor/core")
    ws_url = ha_url.replace("http", "ws", 1) + ("/websocket" if "supervisor" in ha_url else "/api/websocket")
    return {
        "sa": sa,
        "ajan": secenek.get("sohbet_ajani") or os.environ.get("SOHBET_AJANI", "conversation.claude_conversation"),
        "ha_url": ha_url,
        "ws_url": ws_url,
        "ha_token": os.environ.get("HA_TOKEN") or os.environ.get("SUPERVISOR_TOKEN"),
        # HA config klasörü eklentide /homeassistant olarak bağlı; bilgisayarda test ederken yok
        "config_kok": os.environ.get("HA_CONFIG_KOK", "/homeassistant"),
    }

AYAR = ayarlari_oku()
HA = requests.Session()
HA.headers["Authorization"] = "Bearer " + AYAR["ha_token"]

firebase_admin.initialize_app(credentials.Certificate(AYAR["sa"]))
db = firestore.client()
MESAJLAR = db.collection("mesajlar")
DURUM = db.collection("durum")
CIHAZLAR = db.collection("cihazlar")

KAPI_KAMERA = "camera.koridor_kapi_kamera_hd_stream"
SALON_KAMERA = "camera.salon_kamera_hd_stream"
KAPI_ISIK = "light.koridor_kapi_kamera_floodlight_timed"
SIREN = "siren.koridor_kapi_kamera_siren"

# Mesajların altında görünen hazır düğme setleri. "onay": parmak izi ister.
BUTON_SETLERI = {
    "kapi": [
        {"etiket": "📷 Yeni fotoğraf", "komut": "/kapi"},
        {"etiket": "🔦 Kamera ışığı", "komut": "/isik"},
        {"etiket": "🚨 Siren", "komut": "/siren", "onay": True},
    ],
}

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

def kucult(ham, en=1280):
    """JPEG/PNG baytlarını Firestore'a sığacak şekilde küçültüp base64 döndürür."""
    img = Image.open(io.BytesIO(ham)).convert("RGB")
    if img.width > en:
        img = img.resize((en, round(img.height * en / img.width)))
    buf = io.BytesIO()
    for kalite in (75, 60, 45):
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=kalite, optimize=True)
        if buf.tell() < 600_000:  # Firestore belge sınırı 1 MB; base64 ile ~%33 büyür
            break
    return base64.b64encode(buf.getvalue()).decode()

def kamera_resmi(entity_id):
    r = HA.get(f"{AYAR['ha_url']}/api/camera_proxy/{entity_id}", timeout=30)
    r.raise_for_status()
    return kucult(r.content)

def dosya_resmi(yol):
    """HA'daki /config/... yolunu eklentideki /homeassistant/... yoluna çevirip okur."""
    yerel = os.path.join(AYAR["config_kok"], yol.removeprefix("/config/").lstrip("/"))
    with open(yerel, "rb") as f:
        return kucult(f.read())

# ---------------------------------------------------------------- telefona bildirim (FCM)
def bildirim_gonder(mesaj_id, baslik, metin, kanal="genel"):
    jetonlar = [d.id for d in CIHAZLAR.stream()]
    if not jetonlar:
        log.warning("kayıtlı telefon yok, bildirim gönderilmedi")
        return
    for jeton in jetonlar:
        try:
            messaging.send(messaging.Message(
                token=jeton,
                # Sadece veri mesajı: telefon bildirimi kendisi çizer (fotoğraf + düğmeler)
                data={"tur": "mesaj", "mesaj_id": mesaj_id, "baslik": baslik, "metin": metin[:900], "kanal": kanal},
                android=messaging.AndroidConfig(priority="high", ttl=600),
            ))
        except (messaging.UnregisteredError, messaging.SenderIdMismatchError):
            log.info("geçersiz telefon jetonu silindi")
            CIHAZLAR.document(jeton).delete()
        except Exception:  # noqa: BLE001
            log.exception("bildirim gönderilemedi")

def ev_mesaji(veri, bildirim=None):
    """Sohbete {kimden:'ev'} mesajı ekler; istenirse telefona bildirim de gönderir."""
    veri = dict(veri)
    veri.update({"kimden": "ev", "zaman": firestore.SERVER_TIMESTAMP, "durum": "tamam"})
    _, ref = MESAJLAR.add(veri)
    if bildirim:
        bildirim_gonder(ref.id, bildirim.get("baslik", "İsmet Ev"), veri.get("metin", ""), bildirim.get("kanal", "genel"))
    return ref.id

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
    return {"metin": f"🚪 Kapı kamerası ({time.strftime('%H:%M')})", "resim": kamera_resmi(KAPI_KAMERA),
            "butonlar": BUTON_SETLERI["kapi"]}

def komut_salon(_):
    if ha_durum("switch.salon_kamera_privacy")["state"] == "on":
        return {"metin": "🙈 Salon kamerası gizlilik modunda, görüntü alınamıyor."}
    return {"metin": f"🛋️ Salon kamerası ({time.strftime('%H:%M')})", "resim": kamera_resmi(SALON_KAMERA)}

def komut_isik(_):
    ha_servis("light", "toggle", {"entity_id": KAPI_ISIK})
    time.sleep(2)
    acik = ha_durum(KAPI_ISIK)["state"] == "on"
    return {"metin": f"🔦 Kapı kamerası ışığı {'AÇIK' if acik else 'KAPALI'}"}

def komut_siren(istek):
    if not istek.get("onayli"):
        return {"metin": "🔒 Siren için parmak izi onayı gerekli.",
                "butonlar": [{"etiket": "🚨 Siren", "komut": "/siren", "onay": True}]}
    ha_servis("siren", "turn_on", {"entity_id": SIREN})
    return {"metin": "🚨 Siren çalıyor.", "butonlar": [{"etiket": "🔇 Sireni durdur", "komut": "/siren_kapat"}]}

def komut_siren_kapat(_):
    ha_servis("siren", "turn_off", {"entity_id": SIREN})
    return {"metin": "🔇 Siren durduruldu."}

def komut_yeni_sohbet(_):
    DURUM.document("sohbet").delete()
    return {"metin": "🧹 Yeni sohbet başladı, önceki konuşmayı unuttum."}

KOMUTLAR = {"/kapi": komut_kapi, "/salon": komut_salon, "/isik": komut_isik, "/siren": komut_siren,
            "/siren_kapat": komut_siren_kapat, "/yeni": komut_yeni_sohbet}

# ---------------------------------------------------------------- mesaj işleyici
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
            cevap = f(veri) if f else {"metin": "Bu komutu bilmiyorum: " + metin}
        else:
            cevap = {"metin": claude_sor(metin)}
        durum = "tamam"
    except Exception as e:  # noqa: BLE001 — kullanıcıya her durumda cevap dönülmeli
        log.exception("hata")
        cevap = {"metin": f"⚠️ Bir sorun oldu: {e}"}
        durum = "hata"
    cevap.update({"sesli": bool(veri.get("sesli")), "cevap_verilen": belge.id})
    ev_mesaji(cevap)
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

# ---------------------------------------------------------------- HA olayları -> bildirim
def ha_bildirimi_isle(veri):
    log.info("HA bildirimi: %s", veri.get("baslik"))
    mesaj = {"metin": (veri.get("metin") or "").strip() or veri.get("baslik", "")}
    try:
        if veri.get("resim_dosya"):
            mesaj["resim"] = dosya_resmi(veri["resim_dosya"])
        elif veri.get("kamera"):
            mesaj["resim"] = kamera_resmi(veri["kamera"])
    except Exception:  # noqa: BLE001 — fotoğraf olmasa da bildirim gitsin
        log.exception("bildirim fotoğrafı alınamadı")
    if veri.get("butonlar") in BUTON_SETLERI:
        mesaj["butonlar"] = BUTON_SETLERI[veri["butonlar"]]
    if veri.get("baslik"):
        mesaj["baslik"] = veri["baslik"]
    ev_mesaji(mesaj, bildirim={"baslik": veri.get("baslik") or "İsmet Ev", "kanal": veri.get("kanal", "genel")})

def ha_olay_dinleyici():
    """HA websocket'inde `ismet_ev_bildirim` olaylarını dinler; koparsa yeniden bağlanır."""
    while True:
        try:
            ws = websocket.create_connection(AYAR["ws_url"], timeout=30)
            ws.recv()
            ws.send(json.dumps({"type": "auth", "access_token": AYAR["ha_token"]}))
            if json.loads(ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("HA websocket kimlik doğrulaması başarısız")
            ws.send(json.dumps({"id": 1, "type": "subscribe_events", "event_type": "ismet_ev_bildirim"}))
            ws.settimeout(None)
            log.info("HA bildirim olayları dinleniyor")
            while True:
                m = json.loads(ws.recv())
                if m.get("type") == "event":
                    threading.Thread(target=ha_bildirimi_isle, args=(m["event"].get("data") or {},), daemon=True).start()
        except Exception:  # noqa: BLE001
            log.exception("HA websocket koptu, 10 sn sonra yeniden bağlanılacak")
            time.sleep(10)

# ---------------------------------------------------------------- çalıştır
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
    for hedef in (isci, kalp_atisi, ha_olay_dinleyici):
        threading.Thread(target=hedef, daemon=True).start()
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
