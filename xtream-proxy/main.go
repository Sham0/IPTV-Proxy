package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	upstream     = "http://line.iptvhunt.com"
	listenPort   = "8889"
	proxyHost    = "192.168.0.11"
	cacheTTL     = 4 * time.Hour
	epgCachePath = "/storage/proxy/epg-cache.xml.gz"
	credsPath    = "/storage/proxy/creds.txt"
	xmltvfrURL   = "https://xmltvfr.fr/xmltv/xmltv_fr.xml.gz"
	epgRefresh   = 8 * time.Hour
)

// Client avec timeout pour les appels upstream (Xtream API + EPG).
// Sans timeout, http.Get peut bloquer indéfiniment si le serveur accroche,
// ce qui tue silencieusement la goroutine epgFetcher.
var httpClient = &http.Client{Timeout: 5 * time.Minute}

// Android n'expose pas son store CA aux binaires Linux non-system.
// xmltvfr.fr utilise HTTPS — on skip la vérification TLS pour cette source EPG.
var insecureClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
	Timeout: 5 * time.Minute,
}

// ── JSON API cache ────────────────────────────────────────────────────────────

type cacheEntry struct {
	data      json.RawMessage
	expiresAt time.Time
}

type apiCache struct {
	sync.RWMutex
	entries map[string]*cacheEntry
}

func newCache() *apiCache { return &apiCache{entries: make(map[string]*cacheEntry)} }

func (c *apiCache) get(key string) (json.RawMessage, bool) {
	c.RLock()
	defer c.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.data, true
}

func (c *apiCache) set(key string, data json.RawMessage) {
	c.Lock()
	defer c.Unlock()
	c.entries[key] = &cacheEntry{data: data, expiresAt: time.Now().Add(cacheTTL)}
}

var cache = newCache()

// ── EPG cache ─────────────────────────────────────────────────────────────────

var (
	epgMu   sync.RWMutex
	epgData []byte // gzipped XMLTV

	credsOnce  sync.Once
	epgUser    string
	epgPass    string
	credsReady = make(chan struct{})
)

func loadCredsFromDisk() {
	data, err := os.ReadFile(credsPath)
	if err != nil {
		return
	}
	parts := strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return
	}
	credsOnce.Do(func() {
		epgUser = parts[0]
		epgPass = parts[1]
		close(credsReady)
		log.Printf("EPG: credentials chargés depuis disque")
	})
}

func captureCredentials(r *http.Request) {
	u := r.URL.Query().Get("username")
	p := r.URL.Query().Get("password")
	if u == "" || p == "" {
		return
	}
	credsOnce.Do(func() {
		epgUser = u
		epgPass = p
		close(credsReady)
		os.WriteFile(credsPath, []byte(u+"\n"+p+"\n"), 0600)
	})
}

func loadEPGFromDisk() {
	data, err := os.ReadFile(epgCachePath)
	if err != nil {
		return
	}
	epgMu.Lock()
	epgData = data
	epgMu.Unlock()
	log.Printf("EPG: chargé depuis disque (%d octets)", len(data))
}

// ── Filtrage catégories / streams ─────────────────────────────────────────────

func keepLive(name string) bool {
	return strings.HasPrefix(name, "FR|") ||
		strings.HasPrefix(name, "CDM|")
}

var reYear = regexp.MustCompile(`\s*\(\d{4}\)\s*$`)

// cleanVodName strips provider prefixes ("FR - ", "MULTI - ", etc.) and the
// trailing "(YYYY)" so TMDB gets a bare title to match against. Stripping the
// year is required because source_select uses a substring check: "Garlitsky"
// in "daniel garlitsky" scores 80, whereas "Garlitsky (2025)" does not match.
func cleanVodName(name string) string {
	if i := strings.Index(name, " - "); i >= 0 && i <= 8 {
		name = strings.TrimSpace(name[i+3:])
	}
	name = reYear.ReplaceAllString(name, "")
	return strings.TrimSpace(name)
}

func cleanVodNames(data json.RawMessage) json.RawMessage {
	var streams []map[string]interface{}
	if json.Unmarshal(data, &streams) != nil {
		return data
	}
	for _, s := range streams {
		if n, ok := s["name"].(string); ok {
			s["name"] = cleanVodName(n)
		}
	}
	r, _ := json.Marshal(streams)
	return r
}

func keepVod(name string) bool {
	return strings.HasPrefix(name, "|FR|") ||
		strings.HasPrefix(name, "|MULTI|") ||
		strings.Contains(name, "NETFLIX") ||
		strings.Contains(name, "APPLE+")
}

func fetchJSON(url string) (json.RawMessage, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func fetchJSONCtx(ctx context.Context, url string) (json.RawMessage, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func cachedJSON(url string) (json.RawMessage, error) {
	if d, ok := cache.get(url); ok {
		return d, nil
	}
	d, err := fetchJSON(url)
	if err != nil {
		return nil, err
	}
	cache.set(url, d)
	return d, nil
}

func filterCats(data json.RawMessage, keep func(string) bool) json.RawMessage {
	var cats []map[string]interface{}
	if json.Unmarshal(data, &cats) != nil {
		return data
	}
	out := cats[:0]
	for _, c := range cats {
		if keep(fmt.Sprintf("%v", c["category_name"])) {
			out = append(out, c)
		}
	}
	r, _ := json.Marshal(out)
	return r
}

func keptIDs(data json.RawMessage, keep func(string) bool) map[string]bool {
	var cats []map[string]interface{}
	json.Unmarshal(data, &cats)
	ids := make(map[string]bool)
	for _, c := range cats {
		if keep(fmt.Sprintf("%v", c["category_name"])) {
			ids[fmt.Sprintf("%v", c["category_id"])] = true
		}
	}
	return ids
}

func filterStreams(data json.RawMessage, allowed map[string]bool) json.RawMessage {
	var streams []map[string]interface{}
	if json.Unmarshal(data, &streams) != nil {
		return data
	}
	var out []map[string]interface{}
	for _, s := range streams {
		if allowed[fmt.Sprintf("%v", s["category_id"])] {
			out = append(out, s)
			continue
		}
		if ids, ok := s["category_ids"].([]interface{}); ok {
			for _, id := range ids {
				if allowed[fmt.Sprintf("%v", id)] {
					out = append(out, s)
					break
				}
			}
		}
	}
	r, _ := json.Marshal(out)
	return r
}

func patchServerInfo(data json.RawMessage) json.RawMessage {
	var resp map[string]interface{}
	if json.Unmarshal(data, &resp) != nil {
		return data
	}
	if si, ok := resp["server_info"].(map[string]interface{}); ok {
		si["url"] = proxyHost
		si["port"] = listenPort
		si["https_port"] = listenPort
		si["server_protocol"] = "http"
		si["rtmp_port"] = listenPort
	}
	r, _ := json.Marshal(resp)
	return r
}

// ── Inférence epg_channel_id ──────────────────────────────────────────────────

var epgSuffixes = []string{
	" ᴿᴬᵂ", " ⁶⁰ᶠᵖˢ", " ᴴᴰ", " ᵁᴴᴰ",
	" 4K", " UHD", " FHD", " HD", " LQ", " SD",
	" +1", " +24", " +6H",
}

// normalizeStreamName retire le préfixe catégorie ("FR| ", "BE| "…)
// et les suffixes qualité ("HD", "FHD", "ᴿᴬᵂ"…) pour obtenir un nom canonique.
func normalizeStreamName(name string) string {
	if i := strings.LastIndex(name, "| "); i >= 0 {
		name = name[i+2:]
	}
	for {
		trimmed := strings.TrimSpace(name)
		upper := strings.ToUpper(trimmed)
		changed := false
		for _, s := range epgSuffixes {
			if strings.HasSuffix(upper, strings.ToUpper(s)) {
				name = trimmed[:len(trimmed)-len(s)]
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	// Supprime les espaces pour unifier "TV5MONDE" et "TV5 MONDE", "CINE+CLASSIC" et "CINE+ CLASSIC"
	return strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(name)), " ", "")
}

// inferMissingEpgIDs comble les epg_channel_id vides en cherchant
// une variante du même nom qui dispose déjà d'un ID valide.
func inferMissingEpgIDs(data json.RawMessage) json.RawMessage {
	var streams []map[string]interface{}
	if json.Unmarshal(data, &streams) != nil {
		return data
	}

	nameToID := make(map[string]string)
	for _, s := range streams {
		id := fmt.Sprintf("%v", s["epg_channel_id"])
		if id == "" || id == "<nil>" || id == "null" {
			continue
		}
		name := normalizeStreamName(fmt.Sprintf("%v", s["name"]))
		if name != "" {
			if _, exists := nameToID[name]; !exists {
				nameToID[name] = id
			}
		}
	}

	changed := false
	for _, s := range streams {
		id := fmt.Sprintf("%v", s["epg_channel_id"])
		if id != "" && id != "<nil>" && id != "null" {
			continue
		}
		name := normalizeStreamName(fmt.Sprintf("%v", s["name"]))
		if inferred, ok := nameToID[name]; ok {
			s["epg_channel_id"] = inferred
			changed = true
		}
	}
	if !changed {
		return data
	}
	r, _ := json.Marshal(streams)
	return r
}

func catURL(r *http.Request, action string) string {
	u := r.URL.Query()
	return fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=%s",
		upstream, u.Get("username"), u.Get("password"), action)
}

// ── EPG XML streaming ─────────────────────────────────────────────────────────

func xmlAttr(start xml.StartElement, name string) string {
	for _, a := range start.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// copyXMLElement copie l'élément courant (start + contenu + end) vers enc.
func copyXMLElement(dec *xml.Decoder, enc *xml.Encoder, start xml.StartElement) {
	enc.EncodeToken(start)
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			// Ferme les éléments ouverts pour ne pas corrompre l'encodeur
			for depth > 0 {
				enc.EncodeToken(xml.EndElement{Name: start.Name})
				depth--
			}
			return
		}
		enc.EncodeToken(tok)
		switch tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
}

// filterXMLTV lit un flux XMLTV depuis r, écrit dans enc les <channel> et
// <programme> dont l'id/channel est dans wanted.
// Retourne le set des channel IDs effectivement trouvés.
func filterXMLTV(r io.Reader, wanted map[string]bool, enc *xml.Encoder) map[string]bool {
	found := make(map[string]bool)
	dec := xml.NewDecoder(r)
	dec.Strict = false
	dec.Entity = xml.HTMLEntity // gère &eacute; &agrave; &nbsp; etc. dans les EPG

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // entité inconnue ou token corrompu : on passe au suivant
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "channel":
			id := xmlAttr(start, "id")
			if wanted[id] {
				found[id] = true
				copyXMLElement(dec, enc, start)
			} else {
				dec.Skip()
			}
		case "programme":
			ch := xmlAttr(start, "channel")
			if wanted[ch] {
				copyXMLElement(dec, enc, start)
			} else {
				dec.Skip()
			}
		default:
			// <tv> et autres éléments racine : on lit juste les enfants
		}
	}
	return found
}

// ── Refresh EPG ───────────────────────────────────────────────────────────────

func refreshEPG(ctx context.Context) error {
	log.Printf("EPG: début du refresh")

	// 1. Récupère les streams live filtrés → liste des epg_channel_id voulus
	catsURL := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_live_categories",
		upstream, epgUser, epgPass)
	streamsURL := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_live_streams",
		upstream, epgUser, epgPass)

	catsData, err := fetchJSONCtx(ctx, catsURL)
	if err != nil {
		return fmt.Errorf("fetch categories: %w", err)
	}
	streamsData, err := fetchJSONCtx(ctx, streamsURL)
	if err != nil {
		return fmt.Errorf("fetch streams: %w", err)
	}

	filtered := filterStreams(streamsData, keptIDs(catsData, keepLive))
	var streams []map[string]interface{}
	json.Unmarshal(filtered, &streams)

	wantedIDs := make(map[string]bool)
	for _, s := range streams {
		id := fmt.Sprintf("%v", s["epg_channel_id"])
		if id != "" && id != "<nil>" && id != "null" {
			wantedIDs[id] = true
		}
	}
	log.Printf("EPG: %d channel IDs attendus", len(wantedIDs))

	// 2. Construit le XML filtré dans un buffer gzippé
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := xml.NewEncoder(gz)

	enc.EncodeToken(xml.ProcInst{Target: "xml", Inst: []byte(`version="1.0" encoding="UTF-8"`)})
	enc.EncodeToken(xml.StartElement{Name: xml.Name{Local: "tv"}})

	// 2a. Source principale : iptvhunt
	epgURL := fmt.Sprintf("%s/xmltv.php?username=%s&password=%s", upstream, epgUser, epgPass)
	epgReq, _ := http.NewRequestWithContext(ctx, "GET", epgURL, nil)
	resp, err := httpClient.Do(epgReq)
	if err != nil {
		return fmt.Errorf("fetch iptvhunt EPG: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("EPG: iptvhunt status=%d content-type=%q content-encoding=%q size=%s",
		resp.StatusCode,
		resp.Header.Get("Content-Type"),
		resp.Header.Get("Content-Encoding"),
		resp.Header.Get("Content-Length"))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("iptvhunt EPG status %d", resp.StatusCode)
	}

	epgReader := io.Reader(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return fmt.Errorf("gunzip iptvhunt: %w", err)
		}
		defer gr.Close()
		epgReader = gr
	}

	foundIDs := filterXMLTV(epgReader, wantedIDs, enc)
	log.Printf("EPG: %d/%d chaînes trouvées dans iptvhunt", len(foundIDs), len(wantedIDs))

	if len(foundIDs) == 0 {
		return fmt.Errorf("iptvhunt EPG a retourné 0 chaînes (rate-limit ou erreur serveur) — cache conservé")
	}

	// 2b. Source complémentaire : xmltv.fr pour les chaînes manquantes
	missingIDs := make(map[string]bool)
	for id := range wantedIDs {
		if !foundIDs[id] {
			missingIDs[id] = true
		}
	}

	if len(missingIDs) > 0 {
		log.Printf("EPG: %d chaînes manquantes, fetch xmltvfr", len(missingIDs))
		req2, _ := http.NewRequestWithContext(ctx, "GET", xmltvfrURL, nil)
		resp2, err := insecureClient.Do(req2)
		if err != nil {
			log.Printf("EPG: xmltvfr fetch error: %v", err)
		} else {
			defer resp2.Body.Close()
			gr2, err := gzip.NewReader(resp2.Body)
			if err != nil {
				log.Printf("EPG: xmltvfr gunzip error: %v", err)
			} else {
				defer gr2.Close()
				added := filterXMLTV(gr2, missingIDs, enc)
				log.Printf("EPG: %d chaînes ajoutées depuis xmltvfr", len(added))
			}
		}
	}

	enc.EncodeToken(xml.EndElement{Name: xml.Name{Local: "tv"}})
	enc.Flush()
	gz.Close()

	data := buf.Bytes()
	log.Printf("EPG: refresh terminé — %d octets gzippés", len(data))

	// 3. Écrit sur disque
	if err := os.WriteFile(epgCachePath, data, 0644); err != nil {
		log.Printf("EPG: erreur écriture disque: %v", err)
	}

	// 4. Swap atomique en mémoire
	epgMu.Lock()
	epgData = data
	epgMu.Unlock()

	return nil
}

func safeRefreshEPGCtx(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("EPG: panic dans refreshEPG: %v", r)
		}
	}()
	if err := refreshEPG(ctx); err != nil {
		log.Printf("EPG: refresh échoué: %v", err)
	}
}

// ── Refresh EPG déclenché par ticker et par requête ───────────────────────────
// refreshMu protège refreshCancel/refreshStart, accessibles depuis le ticker
// ET depuis handleXMLTV (déclenchement sur requête, fallback si Doze gèle le ticker).
var (
	refreshMu     sync.Mutex
	refreshCancel context.CancelFunc
	refreshStart  time.Time
)

const maxRefreshDuration = 10 * time.Minute

// tryStartRefresh démarre un refresh EPG en arrière-plan si le cache est périmé
// et qu'aucun refresh n'est déjà en cours. No-op si les credentials ne sont pas prêts.
func tryStartRefresh() {
	select {
	case <-credsReady:
	default:
		return
	}

	refreshMu.Lock()
	if refreshCancel != nil && time.Since(refreshStart) > maxRefreshDuration {
		log.Printf("EPG: refresh bloqué depuis >10min, annulation")
		refreshCancel()
		refreshCancel = nil
	}
	if refreshCancel != nil {
		refreshMu.Unlock()
		return
	}
	info, err := os.Stat(epgCachePath)
	if err != nil || time.Since(info.ModTime()) >= epgRefresh {
		ctx, cancel := context.WithCancel(context.Background())
		refreshCancel = cancel
		refreshStart = time.Now()
		refreshMu.Unlock()
		go func() {
			defer func() {
				cancel()
				refreshMu.Lock()
				refreshCancel = nil
				refreshMu.Unlock()
			}()
			safeRefreshEPGCtx(ctx)
		}()
	} else {
		refreshMu.Unlock()
	}
}

func epgFetcher() {
	<-credsReady
	log.Printf("EPG: credentials capturés, vérification du cache")

	info, err := os.Stat(epgCachePath)
	if err != nil {
		log.Printf("EPG: pas de cache disque, refresh au prochain tick")
	} else {
		log.Printf("EPG: cache disque %.1fh, refresh au prochain tick si >= 8h",
			time.Since(info.ModTime()).Hours())
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		tryStartRefresh()
	}
}

// ── Handlers HTTP ─────────────────────────────────────────────────────────────

func handleAPI(w http.ResponseWriter, r *http.Request) {
	captureCredentials(r)
	tryStartRefresh()
	action := r.URL.Query().Get("action")
	upURL := upstream + r.URL.RequestURI()
	w.Header().Set("Content-Type", "application/json")

	switch action {
	case "":
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(patchServerInfo(d))

	case "get_live_categories":
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(filterCats(d, keepLive))

	case "get_live_streams":
		cats, err := cachedJSON(catURL(r, "get_live_categories"))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(inferMissingEpgIDs(filterStreams(d, keptIDs(cats, keepLive))))

	case "get_vod_categories":
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(filterCats(d, keepVod))

	case "get_vod_streams":
		cats, err := cachedJSON(catURL(r, "get_vod_categories"))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(cleanVodNames(filterStreams(d, keptIDs(cats, keepVod))))

	case "get_series_categories":
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(filterCats(d, keepVod))

	case "get_series":
		cats, err := cachedJSON(catURL(r, "get_series_categories"))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		d, err := cachedJSON(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Write(cleanVodNames(filterStreams(d, keptIDs(cats, keepVod))))

	default:
		resp, err := http.Get(upURL)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func handleXMLTV(w http.ResponseWriter, r *http.Request) {
	captureCredentials(r)
	tryStartRefresh() // déclenche un refresh si cache périmé, même si le ticker est gelé par Doze

	epgMu.RLock()
	data := epgData
	epgMu.RUnlock()

	if data == nil {
		// Cache pas encore prêt : redirect transparent vers upstream
		http.Redirect(w, r, upstream+r.URL.RequestURI(), http.StatusFound)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func handleStream(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, upstream+r.URL.RequestURI(), http.StatusFound)
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	os.MkdirAll("/storage/proxy", 0755)
	lf, _ := os.OpenFile("/storage/proxy/proxy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetOutput(lf)
	log.Printf("xtream-proxy starting on :%s", listenPort)

	loadCredsFromDisk()
	loadEPGFromDisk()
	go epgFetcher()

	mux := http.NewServeMux()
	mux.HandleFunc("/player_api.php", handleAPI)
	mux.HandleFunc("/xmltv.php", handleXMLTV)
	mux.HandleFunc("/", handleStream)

	if err := http.ListenAndServe(":"+listenPort, mux); err != nil {
		log.Fatal(err)
	}
}
