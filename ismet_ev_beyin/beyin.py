"""İsmet Ev Beyin — Firebase (Firestore + FCM) <-> Home Assistant köprüsü.

Akış:
  Telefon `mesajlar` koleksiyonuna {kimden:'ben', durum:'yeni', metin, sesli} yazar.
  Beyin bunu dinler, komutsa (/kapi gibi) kendisi yapar, değilse HA'daki Claude
  sohbet ajanına sorar; cevabı {kimden:'ev'} olarak yazar, isteği 'tamam' işaretler.

  HA'dan gelenler (websocket):
    `ismet_ev_bildirim` olayı -> telefona bildirim
        event_data: baslik, metin, resim_dosya (/config/... ops.), kamera (ops.), butonlar ('kapi'), kanal
    `ismet_ev_gorev` olayı (script.ismet_gorev_kur'dan, Claude çağırır) -> zamanlanmış görev
        event_data: yapilacak, dakika_sonra | saat (HH:MM), haber_ver
    Takip cihazı evden çıkınca -> evi tara, Claude ile doğal dilde rapor + bildirim

Çalışma ortamı:
  HA eklentisi: /data/options.json + SUPERVISOR_TOKEN (otomatik), /homeassistant = HA config (salt okunur).
  Bilgisayarda test: HA_URL, HA_TOKEN, SERVIS_HESABI_DOSYA ortam değişkenleri.
"""
import base64
import datetime as dt
import io
import json
import logging
import os
import queue
import re
import threading
import time
from zoneinfo import ZoneInfo

import firebase_admin
import requests
import websocket
from firebase_admin import credentials, firestore, messaging
from google.cloud.firestore_v1.base_query import FieldFilter
from PIL import Image

SURUM = "0.3.0"
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
        "takip": secenek.get("takip_cihazi") or os.environ.get("TAKIP_CIHAZI", "device_tracker.ev_indoor"),
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
GOREVLER = db.collection("gorevler")

KAPI_KAMERA = "camera.koridor_kapi_kamera_hd_stream"
SALON_KAMERA = "camera.salon_kamera_hd_stream"
KAPI_ISIK = "light.koridor_kapi_kamera_floodlight_timed"
SIREN = "siren.koridor_kapi_kamera_siren"
# Raporda "evde biri var" sayılmayacak kişiler (kullanıcının kendisi ve sanal kişiler)
RAPOR_HARIC_KISILER = {"person.igurler", "person.mqtt", "person.tablet"}
# Görevlere eklenemeyecek riskli işler: bunlar her zaman parmak iziyle elle yapılır
RISKLI = re.compile(r"siren|alarm|kilit|kapıyı aç|kapiyi ac", re.IGNORECASE)

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

def ha_tum_durumlar():
    r = HA.get(f"{AYAR['ha_url']}/api/states", timeout=30)
    r.raise_for_status()
    return r.json()

def ha_servis(domain, servis, veri, cevapli=False):
    url = f"{AYAR['ha_url']}/api/services/{domain}/{servis}" + ("?return_response" if cevapli else "")
    r = HA.post(url, json=veri, timeout=90)
    r.raise_for_status()
    return r.json()

_saat_dilimi = None
def saat_dilimi():
    """HA'nın saat dilimi (ör. Europe/Istanbul)."""
    global _saat_dilimi
    if _saat_dilimi is None:
        try:
            r = HA.get(f"{AYAR['ha_url']}/api/config", timeout=15)
            _saat_dilimi = ZoneInfo(r.json().get("time_zone") or "Europe/Istanbul")
        except Exception:  # noqa: BLE001
            _saat_dilimi = ZoneInfo("Europe/Istanbul")
    return _saat_dilimi

def simdi():
    return dt.datetime.now(saat_dilimi())

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
def claude_sor(metin, sohbet="devam"):
    """HA'daki Claude ajanına sorar.
    sohbet='devam': kayıtlı sohbete devam et ve kaydet (normal mesajlar)
    sohbet='yeni':  yeni sohbet başlat ve kaydet (rapor gibi; kullanıcının cevabı bu bağlamda sürsün)
    sohbet='ayri':  bağımsız tek seferlik (görev çalıştırma); kayıtlı sohbete dokunma
    """
    kayit_ref = DURUM.document("sohbet")
    veri = {"text": metin, "agent_id": AYAR["ajan"], "language": "tr"}
    if sohbet == "devam":
        kayit = kayit_ref.get().to_dict() or {}
        # 30 dakikadan eski sohbeti devam ettirme; boş conversation_id gönderme (hafıza bozulur)
        if kayit.get("conversation_id") and time.time() - kayit.get("zaman", 0) < 1800:
            veri["conversation_id"] = kayit["conversation_id"]
    cevap = ha_servis("conversation", "process", veri, cevapli=True)["service_response"]
    if sohbet != "ayri" and cevap.get("conversation_id"):
        kayit_ref.set({"conversation_id": cevap["conversation_id"], "zaman": time.time()})
    return (cevap.get("response", {}).get("speech", {}).get("plain", {}).get("speech") or "").strip() or "(boş cevap)"

# ---------------------------------------------------------------- evden çıkış raporu
def ev_taramasi():
    """Açık kalan ve dikkat edilmesi gereken şeyleri toplar."""
    durumlar = ha_tum_durumlar()
    ad = lambda s: s["attributes"].get("friendly_name") or s["entity_id"]  # noqa: E731
    t = {"isiklar": [], "klimalar": [], "medya": [], "varlik": [], "kisiler": []}
    for s in durumlar:
        e, st, a = s["entity_id"], s["state"], s["attributes"]
        alan = e.split(".")[0]
        if alan == "light" and st == "on":
            t["isiklar"].append((e, ad(s)))
        elif alan == "climate" and st not in ("off", "unavailable", "unknown"):
            t["klimalar"].append((e, ad(s)))
        elif alan == "media_player" and st in ("on", "playing", "paused"):
            t["medya"].append((e, ad(s)))
        elif alan == "binary_sensor" and st == "on" and a.get("device_class") in ("occupancy", "presence", "motion") \
                and "kapi_kamera" not in e:
            t["varlik"].append((e, ad(s)))
        elif alan == "person" and st == "home" and e not in RAPOR_HARIC_KISILER:
            t["kisiler"].append((e, ad(s)))
    return t

def rapor_gonder(sebep="cikis"):
    t = ev_taramasi()
    kapatilabilir = [e for e, _ in t["isiklar"] + t["klimalar"] + t["medya"]]
    DURUM.document("rapor").set({"kapatilabilir": kapatilabilir, "zaman": time.time()})
    liste = lambda k: ", ".join(n for _, n in t[k]) or "yok"  # noqa: E731
    giris = "İsmet az önce evden çıktı." if sebep == "cikis" else "İsmet evin şu anki durumunu soruyor."
    istem = (
        f"[SİSTEM NOTU — bu mesajı İsmet yazmadı; ona doğrudan, telefonda konuşur gibi hitap et] {giris}\n"
        f"Evin durumu:\n- Açık ışıklar: {liste('isiklar')}\n- Çalışan klima/termostat: {liste('klimalar')}\n"
        f"- Açık TV/medya: {liste('medya')}\n- Hareket/varlık algılanan yerler: {liste('varlik')}\n"
        f"- Evde olan diğer kişiler: {liste('kisiler')}\n"
        "Ona 2-4 cümlelik, samimi ve doğal bir Türkçe mesaj yaz; emoji, başlık ve liste kullanma. "
        "Açık kalanları söyle. Evde başka kimse yoksa ve açık bir şey varsa kapatmamı isteyip istemediğini sor. "
        "Evde biri varsa ya da hareket algılandıysa bunu belirt ve ışıkları kapatmayı önerme. "
        "Bu mesajda hiçbir cihazı değiştirme, sadece bilgi ver ve sor."
    )
    metin = claude_sor(istem, sohbet="yeni")
    mesaj = {"metin": metin, "baslik": "🏠 Ev raporu", "sesli": True}
    if kapatilabilir and not t["kisiler"]:
        mesaj["butonlar"] = [{"etiket": "💡 Hepsini kapat", "komut": "/hepsini_kapat"},
                             {"etiket": "👍 Kalsın", "komut": "/kalsin"}]
    ev_mesaji(mesaj, bildirim={"baslik": "🏠 Ev raporu", "kanal": "genel"})
    log.info("ev raporu gönderildi (%s), kapatılabilir: %d", sebep, len(kapatilabilir))

# ---------------------------------------------------------------- görevler
def gorev_zamani(veri):
    """dakika_sonra veya saat (HH:MM) alanından çalışma zamanını (epoch) hesaplar."""
    dk = veri.get("dakika_sonra")
    if dk not in (None, "", 0, "0"):
        return time.time() + float(dk) * 60
    saat = str(veri.get("saat") or "").strip()
    m = re.fullmatch(r"(\d{1,2})[:.](\d{2})", saat)
    if not m:
        raise ValueError("Görev için ne zaman yapılacağı anlaşılamadı (dakika_sonra ya da saat gerekli).")
    s = simdi()
    hedef = s.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)
    if hedef <= s:
        hedef += dt.timedelta(days=1)
    return hedef.timestamp()

def saat_yazisi(ts):
    z = dt.datetime.fromtimestamp(ts, saat_dilimi())
    gun = "" if z.date() == simdi().date() else "yarın "
    return f"{gun}{z:%H:%M}"

def gorev_kur(veri):
    yapilacak = (veri.get("yapilacak") or "").strip()
    if not yapilacak:
        raise ValueError("Görevde ne yapılacağı boş.")
    if RISKLI.search(yapilacak):
        ev_mesaji({"metin": "🔒 Siren, alarm ve kilit gibi işler göreve eklenemez; bunları her seferinde parmak izinle sen yaparsın."})
        return
    ts = gorev_zamani(veri)
    haber = str(veri.get("haber_ver", True)).lower() not in ("false", "0", "hayır", "no")
    _, ref = GOREVLER.add({"yapilacak": yapilacak, "zaman": ts, "haber_ver": haber, "durum": "bekliyor",
                           "olusturma": time.time()})
    log.info("görev kuruldu %s: %s @ %s", ref.id, yapilacak, saat_yazisi(ts))
    ev_mesaji({"metin": f"📋 Görev kuruldu: {saat_yazisi(ts)} — {yapilacak}",
               "butonlar": [{"etiket": "❌ İptal", "komut": f"/gorev_iptal {ref.id}"}]})

def gorev_calistir(belge):
    g = belge.to_dict()
    log.info("görev çalışıyor %s: %s", belge.id, g["yapilacak"])
    try:
        sonuc = claude_sor(
            f"[ZAMANLANMIŞ GÖREV — İsmet bunu daha önce istedi, şimdi zamanı geldi] Şunu yap: {g['yapilacak']}. "
            "Yaptıktan sonra ne yaptığını İsmet'e tek kısa cümleyle söyle.", sohbet="ayri")
        metin = f"⏰ {sonuc}"
    except Exception as e:  # noqa: BLE001
        log.exception("görev hatası")
        metin = f"⚠️ Görev yapılamadı ({g['yapilacak']}): {e}"
    if g.get("haber_ver", True):
        ev_mesaji({"metin": metin, "sesli": True}, bildirim={"baslik": "⏰ Görev tamam", "kanal": "genel"})
    else:
        ev_mesaji({"metin": metin})
    belge.reference.delete()  # görev bitti, kendini siler

def gorev_zamanlayici():
    while True:
        try:
            for b in GOREVLER.where(filter=FieldFilter("durum", "==", "bekliyor")).stream():
                if b.to_dict().get("zaman", 0) <= time.time():
                    b.reference.update({"durum": "calisiyor"})  # bir sonraki turda tekrar başlamasın
                    threading.Thread(target=gorev_calistir, args=(b,), daemon=True).start()
        except Exception:  # noqa: BLE001
            log.exception("zamanlayıcı hatası")
        time.sleep(10)

# ---------------------------------------------------------------- komutlar
def komut_kapi(_):
    return {"metin": f"🚪 Kapı kamerası ({simdi():%H:%M})", "resim": kamera_resmi(KAPI_KAMERA),
            "butonlar": BUTON_SETLERI["kapi"]}

def komut_salon(_):
    if ha_durum("switch.salon_kamera_privacy")["state"] == "on":
        return {"metin": "🙈 Salon kamerası gizlilik modunda, görüntü alınamıyor."}
    return {"metin": f"🛋️ Salon kamerası ({simdi():%H:%M})", "resim": kamera_resmi(SALON_KAMERA)}

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

def komut_rapor(_):
    rapor_gonder(sebep="istek")
    return None  # cevabı rapor_gonder kendisi yazdı

def komut_hepsini_kapat(_):
    liste = (DURUM.document("rapor").get().to_dict() or {}).get("kapatilabilir", [])
    if not liste:
        return {"metin": "Kapatılacak bir şey kalmamış."}
    for e in liste:
        alan = e.split(".")[0]
        try:
            ha_servis(alan, "turn_off", {"entity_id": e})
        except Exception:  # noqa: BLE001
            log.exception("kapatılamadı: %s", e)
    DURUM.document("rapor").delete()
    return {"metin": f"💡 Tamam, açık kalan {len(liste)} cihazı kapattım.", "sesli": True}

def komut_kalsin(_):
    return {"metin": "👍 Peki, dokunmuyorum."}

def komut_gorevler(_):
    gorevler = sorted((b for b in GOREVLER.where(filter=FieldFilter("durum", "==", "bekliyor")).stream()),
                      key=lambda b: b.to_dict()["zaman"])
    if not gorevler:
        return {"metin": "📋 Bekleyen görev yok. Bana \"1 saat sonra klimayı kapat\" gibi bir şey söyleyerek görev kurabilirsin."}
    satirlar = [f"• {saat_yazisi(b.to_dict()['zaman'])} — {b.to_dict()['yapilacak']}" for b in gorevler]
    return {"metin": "📋 Bekleyen görevler:\n" + "\n".join(satirlar),
            "butonlar": [{"etiket": f"❌ {saat_yazisi(b.to_dict()['zaman'])} iptal", "komut": f"/gorev_iptal {b.id}"}
                         for b in gorevler]}

def komut_gorev_iptal(istek):
    parca = (istek.get("metin") or "").split()
    if len(parca) < 2:
        return {"metin": "Hangi görev? /gorevler ile listeyi aç."}
    ref = GOREVLER.document(parca[1])
    g = ref.get()
    if not g.exists:
        return {"metin": "Bu görev artık yok (zaten çalışmış ya da iptal edilmiş)."}
    ref.delete()
    return {"metin": f"❌ Görev iptal edildi: {g.to_dict()['yapilacak']}"}

KOMUTLAR = {"/kapi": komut_kapi, "/salon": komut_salon, "/isik": komut_isik, "/siren": komut_siren,
            "/siren_kapat": komut_siren_kapat, "/yeni": komut_yeni_sohbet, "/rapor": komut_rapor,
            "/hepsini_kapat": komut_hepsini_kapat, "/kalsin": komut_kalsin, "/gorevler": komut_gorevler,
            "/gorev_iptal": komut_gorev_iptal}

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
    if cevap is not None:
        cevap.setdefault("sesli", bool(veri.get("sesli")))
        cevap["cevap_verilen"] = belge.id
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

# ---------------------------------------------------------------- HA olayları
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

def guvenli(f, *args):
    """Arka plan işini çalıştırır; hata olursa kaydeder ve telefona bildirir."""
    try:
        f(*args)
    except Exception as e:  # noqa: BLE001
        log.exception("%s hatası", f.__name__)
        ev_mesaji({"metin": f"⚠️ Bir sorun oldu: {e}"})

ABONELIK_BILDIRIM, ABONELIK_GOREV, ABONELIK_CIKIS = 1, 2, 3

def ha_olay_dinleyici():
    """HA websocket'inde olayları ve evden çıkışı dinler; koparsa yeniden bağlanır."""
    while True:
        try:
            ws = websocket.create_connection(AYAR["ws_url"], timeout=30)
            ws.recv()
            ws.send(json.dumps({"type": "auth", "access_token": AYAR["ha_token"]}))
            if json.loads(ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("HA websocket kimlik doğrulaması başarısız")
            ws.send(json.dumps({"id": ABONELIK_BILDIRIM, "type": "subscribe_events", "event_type": "ismet_ev_bildirim"}))
            ws.send(json.dumps({"id": ABONELIK_GOREV, "type": "subscribe_events", "event_type": "ismet_ev_gorev"}))
            # Evden çıkış: 2 dakika boyunca dışarıda kalırsa (GPS sıçramalarını yok say)
            ws.send(json.dumps({"id": ABONELIK_CIKIS, "type": "subscribe_trigger", "trigger": {
                "platform": "state", "entity_id": AYAR["takip"], "from": "home", "to": "not_home",
                "for": {"minutes": 2}}}))
            ws.settimeout(None)
            log.info("HA olayları dinleniyor (takip: %s)", AYAR["takip"])
            while True:
                m = json.loads(ws.recv())
                if m.get("type") != "event":
                    continue
                if m["id"] == ABONELIK_BILDIRIM:
                    threading.Thread(target=guvenli, args=(ha_bildirimi_isle, m["event"].get("data") or {}), daemon=True).start()
                elif m["id"] == ABONELIK_GOREV:
                    threading.Thread(target=guvenli, args=(gorev_kur, m["event"].get("data") or {}), daemon=True).start()
                elif m["id"] == ABONELIK_CIKIS:
                    threading.Thread(target=guvenli, args=(rapor_gonder, "cikis"), daemon=True).start()
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
    for hedef in (isci, kalp_atisi, ha_olay_dinleyici, gorev_zamanlayici):
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
