# İsmet Ev Beyin

Home Assistant eklentisi: İsmet Ev Android uygulamasının evdeki beyni.

- Telefon ile ev arasında Firebase (Firestore) posta kutusu kullanır; eve dışarıdan açık port gerekmez.
- Mesajları Home Assistant'taki sohbet ajanına (Claude) sorar, cevabı uygulamaya yazar.
- Komutlar: `/kapi`, `/salon`, `/isik`, `/yeni`.

## Kurulum

1. Ayarlar → Eklentiler → Eklenti Mağazası → ⋮ → Depolar → bu deponun adresini ekleyin.
2. "İsmet Ev Beyin" eklentisini kurun.
3. Yapılandırma sekmesinde `servis_hesabi` alanına Firebase servis hesabı JSON'unu yapıştırın.
4. Başlatın.

Bu depoda hiçbir anahtar veya şifre yoktur; hepsi eklenti ayarlarında tutulur.
