# IPTV Proxy — Freebox Player (Android TV)

Proxy Xtream Codes API tournant sur un Freebox Player (Android TV, 192.168.0.25).  
Filtre le catalogue du provider IPTV pour ne garder que les contenus FR/MULTI, cache l'EPG localement et l'expose via les mêmes endpoints que le provider.

---

## Architecture

```
Boot
 └─► fr.shamo.m3ubootstart (foreground service)
       └─► libxtreamproxy.so  →  :8889  (proxy Go, Xtream Codes API + EPG)
```

**IPTV Smarters config (Xtream Codes API) :**
- URL : `http://192.168.0.25:8889`
- Username / Password : voir `freebox-config.txt` sur la SD

---

## Composants

### `libxtreamproxy.so` — proxy Go (`xtream-proxy/main.go`)

Binaire Go compilé pour ARM7, packagé en `.so` dans l'APK (contournement SELinux : les native libs ont le contexte `apk_data_file` que l'app peut exécuter).

| Endpoint | Comportement |
|----------|-------------|
| `GET /player_api.php?action=` | Passe la requête upstream, patche `server_info` (url/port → 192.168.0.25:8889) |
| `GET /player_api.php?action=get_live_categories` | Filtre : garde uniquement les catégories FR, 4K, CA FRENCH, BE, EU Luxembourg |
| `GET /player_api.php?action=get_live_streams` | Filtre par catégories live retenues |
| `GET /player_api.php?action=get_vod_categories` | Filtre : FR, QC, MULTI, 4K, Netflix, Apple+ |
| `GET /player_api.php?action=get_vod_streams` | Filtre par catégories VOD retenues |
| `GET /player_api.php?action=get_series*` | Même filtrage que VOD |
| `GET /player_api.php?action=*` (autres) | Pass-through transparent |
| `GET /xmltv.php` | Sert l'EPG depuis cache mémoire (gzippé) — voir section EPG |
| `GET /live/:id` / `GET /movie/:id` / … | Redirect 302 vers upstream |

Cache JSON en mémoire : TTL 4h (évite de re-fetcher le catalogue à chaque requête IPTV Smarters).

**Règles de filtrage :**

```go
// Live : préfixes de catégorie
keepLive: "FR|", "24/7", "4K", "CA| FRENCH", "BE| WALLONIË", "EU| LUXEMBOURG"

// VOD + Séries : préfixes de catégorie
keepVod: "|FR|", "|QC|", "|MULTI|", "4K", contient "NETFLIX", contient "APPLE+"
```

**DNS :** forcé sur 192.168.0.254 (la box) — Android n'expose pas `/etc/resolv.conf` aux binaires Linux.

**Log :** `/sdcard/xtream-proxy.log`

---

### `fr.shamo.m3ubootstart` — APK Android (`android/`)

APK minimaliste qui maintient le proxy vivant après le boot.

| Composant | Rôle |
|-----------|------|
| `BootReceiver` | Écoute `BOOT_COMPLETED`, démarre `ProxyService` |
| `ProxyService` | Foreground service (empêche Android de tuer le processus ~20s après le boot), exécute `libxtreamproxy.so` via `Runtime.exec` |

**Manifest :**
- `targetSdkVersion=25` : évite `NotificationChannel` (requis API 26+) et `foregroundServiceType` (API 29+)
- `android:exported="true"` sur `ProxyService` : permet `am start-foreground-service` depuis ADB

---

## EPG

### Sources
| Source | URL | Usage |
|--------|-----|-------|
| iptvhunt | `http://line.iptvhunt.com/xmltv.php?username=…&password=…` | Source principale (global, tous pays) |
| xmltvfr.fr | `https://xmltvfr.fr/xmltv/xmltv_fr.xml.gz` | Complément FR pour chaînes manquantes |

### Stratégie cache (dans `libxtreamproxy.so`)

- Au démarrage : charge `/sdcard/m3u-proxy-backup/epg-cache.xml.gz` en mémoire → `/xmltv.php` répond instantanément
- Goroutine EPG (toutes les 8h) :
  1. Récupère la liste des `epg_channel_id` des streams live filtrés
  2. Télécharge l'EPG iptvhunt complet, filtre en streaming XML (garde seulement les chaînes du Set)
  3. Merge avec xmltvfr.fr pour les chaînes manquantes
  4. Compresse → écrit disque + swap mémoire atomique
- Fallback si cache vide au 1er boot → redirect transparent vers upstream

**Gain :** ~50 000 chaînes iptvhunt → ~300 chaînes FR. Fichier : ~100 MB brut → ~3 MB gzippé.

### IDs EPG

Les `epg_channel_id` des streams utilisent le format `Name.fr` (ex : `TF1.fr`, `France2.fr`, `Action.fr`).  
C'est le même format qu'iptvhunt et xmltvfr.fr — pas de mapping nécessaire.

---

## Build & Deploy

### Prérequis

```bash
sudo apt install apktool zipalign apksigner golang-go
```

### Compiler le proxy Go

```bash
cd xtream-proxy
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 \
  go build -ldflags="-s -w" -o libxtreamproxy.so .
```

### Mettre à jour l'APK

```bash
# Décompiler
apktool d android/m3ubootstart_v4_clean.apk -o /tmp/m3ubootstart_decompiled -f

# Remplacer le binaire
cp xtream-proxy/libxtreamproxy.so /tmp/m3ubootstart_decompiled/lib/armeabi-v7a/

# Recompiler + signer (keystore sur la SD ou ~/m3u-proxy-backup/v3-xtream/)
apktool b /tmp/m3ubootstart_decompiled -o /tmp/m3ubootstart_unsigned.apk
zipalign -v 4 /tmp/m3ubootstart_unsigned.apk /tmp/m3ubootstart_aligned.apk
apksigner sign \
  --ks m3u-sign.keystore --ks-key-alias m3u \
  --ks-pass pass:android --key-pass pass:android \
  --out m3ubootstart_new.apk /tmp/m3ubootstart_aligned.apk

# Déployer (même keystore = -r, pas besoin de désinstaller)
adb connect 192.168.0.25:5555
adb push m3ubootstart_new.apk /data/local/tmp/m3ubootstart.apk
adb shell 'settings put global package_verifier_enable 0 \
  && pm install -r /data/local/tmp/m3ubootstart.apk \
  && settings put global package_verifier_enable 1'
```

### Keystore

- Local : `~/m3u-proxy-backup/v3-xtream/m3u-sign.keystore`
- SD : `/sdcard/m3u-proxy-backup/m3u-sign.keystore`
- alias : `m3u` / pass : `android`
- **Conserver ce keystore** — en changer force un désinstall + re-grant permissions

---

## Diagnostics

```bash
# Ports actifs
adb shell 'netstat -tln | grep ":::888"'
# Attendu : 8889

# Process
adb shell 'ps -A | grep shamo'

# Logs
adb shell 'tail -20 /sdcard/xtream-proxy.log'

# Forcer redémarrage du service
adb shell 'am start-foreground-service --include-stopped-packages -n fr.shamo.m3ubootstart/.ProxyService'
```

---

## M3U natif (si besoin)

L'API Xtream expose un endpoint M3U sans rien modifier :

```
http://192.168.0.25:8889/get.php?username=…&password=…&type=m3u_plus&output=ts
```

---

## Apps suspendues (boot)

Suspendues via `pm suspend --user 0` — pour réactiver :

```bash
adb shell pm unsuspend --user 0 <package>
```

| Package | App |
|---------|-----|
| `com.canal.android.canal` | Canal+ |
| `com.cbs.ca` | Paramount+ |
| `com.disney.disneyplus` | Disney+ |
| `com.google.android.videos` | Google Play Films |
| `com.google.android.youtube.tvkids` | YouTube Kids |
| `com.internet.tvbrowser` | TV Browser |
| `com.nextinteractive.rmcbfmplay.tv.free` | RMC BFM Play |
| `com.wbd.stream` | Max (HBO) |
| `fr.francetv.pluzz` | France TV |
| `fr.m6.m6replay.free` | M6+ |
| `fr.tfou.max` | Tfou Max |
