package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
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
	proxyPort          = "8889"
	upstream           = "http://line.iptvhunt.com"
	cachePath          = "/storage/proxy/codec_cache.json"
	epgCachePath       = "/storage/proxy/epg-cache.xml.gz"
	credsPath          = "/storage/proxy/creds.txt"
	logPath            = "/storage/proxy/proxy.log"
	epgRefreshInterval = 8 * time.Hour
	codecScanDelay     = 1 * time.Second // was 200ms — reduced Pi3 load during overnight scan
	scanCheckpointN    = 100
)

// --- Credentials ---

type credStore struct {
	mu       sync.RWMutex
	username string
	password string
	ready    chan struct{}
	once     sync.Once
}

var globalCreds = &credStore{ready: make(chan struct{})}

func (c *credStore) capture(u, p string) {
	c.mu.Lock()
	changed := c.username != u || c.password != p
	c.username = u
	c.password = p
	c.mu.Unlock()
	c.once.Do(func() { close(c.ready) })
	if changed {
		os.WriteFile(credsPath, []byte(u+"\n"+p+"\n"), 0600)
		log.Printf("EPG: credentials capturés, vérification du cache")
	}
}

func (c *credStore) get() (string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.username, c.password
}

func (c *credStore) wait() { <-c.ready }

// --- Codec cache ---

type codecDB struct {
	mu   sync.RWMutex
	data map[string]string
}

var codec = &codecDB{data: make(map[string]string)}

func (db *codecDB) load() {
	f, err := os.ReadFile(cachePath)
	if err != nil {
		return
	}
	var m map[string]string
	if json.Unmarshal(f, &m) == nil {
		db.mu.Lock()
		db.data = m
		db.mu.Unlock()
		log.Printf("Codec cache: %d entrées chargées", len(m))
	}
}

func (db *codecDB) save() {
	db.mu.RLock()
	b, _ := json.Marshal(db.data)
	db.mu.RUnlock()
	os.WriteFile(cachePath, b, 0644)
}

func (db *codecDB) get(id string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data[id]
}

func (db *codecDB) set(id, c string) {
	db.mu.Lock()
	db.data[id] = c
	db.mu.Unlock()
}

// --- EPG cache ---

type epgStore struct {
	mu         sync.RWMutex
	data       []byte
	lastUpdate time.Time
}

var epg = &epgStore{}

func (e *epgStore) load() {
	f, err := os.ReadFile(epgCachePath)
	if err != nil {
		return
	}
	fi, _ := os.Stat(epgCachePath)
	e.mu.Lock()
	e.data = f
	if fi != nil {
		e.lastUpdate = fi.ModTime()
	}
	e.mu.Unlock()
	log.Printf("EPG: chargé depuis disque (%d octets)", len(f))
}

func (e *epgStore) get() []byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.data
}

func (e *epgStore) needsRefresh() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.data) == 0 || time.Since(e.lastUpdate) >= epgRefreshInterval
}

func (e *epgStore) age() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return time.Since(e.lastUpdate)
}

func (e *epgStore) refresh(u, p string) {
	log.Printf("EPG: début du refresh")

	// Collect channel IDs from live streams
	resp, err := apiClient.Get(fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_live_streams", upstream, u, p))
	if err != nil {
		log.Printf("EPG: fetch streams error: %v", err)
		return
	}
	var streams []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&streams)
	resp.Body.Close()

	channelIDs := make(map[string]bool)
	for _, s := range streams {
		if id, ok := s["epg_channel_id"].(string); ok && id != "" {
			channelIDs[id] = true
		}
	}
	log.Printf("EPG: %d channel IDs attendus", len(channelIDs))

	// Fetch EPG XML from upstream
	epgResp, err := apiClient.Get(fmt.Sprintf("%s/xmltv.php?username=%s&password=%s", upstream, u, p))
	if err != nil {
		log.Printf("EPG: xmltvfr fetch error: %v", err)
		return
	}
	log.Printf("EPG: iptvhunt status=%d content-type=%q content-encoding=%q size=%s",
		epgResp.StatusCode,
		epgResp.Header.Get("Content-Type"),
		epgResp.Header.Get("Content-Encoding"),
		epgResp.Header.Get("Content-Length"),
	)
	body, err := io.ReadAll(epgResp.Body)
	epgResp.Body.Close()
	if err != nil {
		log.Printf("EPG: read error: %v", err)
		return
	}

	found := 0
	for id := range channelIDs {
		if bytes.Contains(body, []byte(id)) {
			found++
		}
	}
	log.Printf("EPG: %d/%d chaînes trouvées dans iptvhunt", found, len(channelIDs))

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(body)
	gz.Close()
	gzipped := buf.Bytes()

	os.WriteFile(epgCachePath, gzipped, 0644)
	e.mu.Lock()
	e.data = gzipped
	e.lastUpdate = time.Now()
	e.mu.Unlock()

	log.Printf("EPG: refresh terminé — %d octets gzippés", len(gzipped))
}

// --- HTTP clients ---

var apiClient = &http.Client{Timeout: 30 * time.Second}
var streamClient = &http.Client{Timeout: 0}

// --- Filters ---

var reYear = regexp.MustCompile(`\s*\(\d{4}\)\s*$`)

func cleanVodName(name string) string {
	if i := strings.Index(name, " - "); i > 0 && i <= 8 {
		name = name[i+3:]
	}
	return strings.TrimSpace(reYear.ReplaceAllString(name, ""))
}

func cleanVodNames(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = cleanVodName(n)
	}
	return out
}

func isLiveStream(name string) bool {
	upper := strings.ToUpper(name)
	if strings.Contains(upper, "4K") {
		return false
	}
	return strings.Contains(name, "FR|") || strings.Contains(name, "CDM|")
}

func isVodStream(name string) bool {
	if strings.Contains(name, "4K") || strings.HasPrefix(name, "QFR") {
		return false
	}
	return strings.HasPrefix(name, "FR ") || strings.HasPrefix(name, "NF ")
}

var hevcCodecs = []string{"hevc", "h265", "av1", "vp9"}

func isHevc(c string) bool {
	c = strings.ToLower(c)
	for _, h := range hevcCodecs {
		if strings.Contains(c, h) {
			return true
		}
	}
	return false
}

func filterLive(streams []map[string]interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	for _, s := range streams {
		name, _ := s["name"].(string)
		if isLiveStream(name) {
			out = append(out, s)
		}
	}
	return out
}

func filterVod(streams []map[string]interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	for _, s := range streams {
		name, _ := s["name"].(string)
		if !isVodStream(name) {
			continue
		}
		id, _ := s["stream_id"].(json.Number)
		if isHevc(codec.get(id.String())) {
			continue
		}
		cp := make(map[string]interface{}, len(s))
		for k, v := range s {
			cp[k] = v
		}
		cp["name"] = cleanVodName(name)
		out = append(out, cp)
	}
	return out
}

func filterSeries(series []map[string]interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	for _, s := range series {
		name, _ := s["name"].(string)
		if !isVodStream(name) {
			continue
		}
		cp := make(map[string]interface{}, len(s))
		for k, v := range s {
			cp[k] = v
		}
		cp["name"] = cleanVodName(name)
		out = append(out, cp)
	}
	return out
}

// --- Codec scanner ---

func nextScanAt() time.Time {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func codecScanner() {
	next := nextScanAt()
	delay := time.Until(next)
	log.Printf("Codec scanner: premier scan dans %.1fh (à 3h00)", delay.Hours())
	time.Sleep(delay)

	for {
		globalCreds.wait()
		u, p := globalCreds.get()

		resp, err := apiClient.Get(fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_vod_streams", upstream, u, p))
		if err != nil {
			log.Printf("Codec scanner: fetch categories error: %v", err)
		} else {
			var all []map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&all)
			resp.Body.Close()

			filtered := filterVod(all)
			n := len(filtered)
			log.Printf("Codec scanner: %d streams à scanner", n)

			saved := 0
			for i, s := range filtered {
				id, _ := s["stream_id"].(json.Number)
				idStr := id.String()
				if codec.get(idStr) != "" {
					continue
				}

				// Sequential fetch — no goroutines to limit memory and CPU
				infoResp, err := apiClient.Get(fmt.Sprintf(
					"%s/player_api.php?username=%s&password=%s&action=get_vod_info&vod_id=%s",
					upstream, u, p, idStr,
				))
				if err == nil {
					var info map[string]interface{}
					if json.NewDecoder(infoResp.Body).Decode(&info) == nil {
						if mi, ok := info["info"].(map[string]interface{}); ok {
							if c, ok := mi["video_codec"].(string); ok && c != "" {
								codec.set(idStr, strings.ToLower(c))
								saved++
							}
						}
					}
					infoResp.Body.Close()
				}

				if (i+1)%scanCheckpointN == 0 {
					codec.save()
					log.Printf("Codec scanner: %d/%d scannés, %d nouveaux", i+1, n, saved)
				}

				time.Sleep(codecScanDelay)
			}

			codec.save()
			log.Printf("Codec scanner: scan terminé — %d streams, %d nouveaux codecs", n, saved)
		}

		// Next scan: 6 days from now, at 3h00
		base := time.Now().Add(6 * 24 * time.Hour)
		next := time.Date(base.Year(), base.Month(), base.Day()+1, 3, 0, 0, 0, base.Location())
		log.Printf("Codec scanner: prochain scan dans %.1fh", time.Until(next).Hours())
		time.Sleep(time.Until(next))
	}
}

// --- EPG background refresher ---

func epgTicker() {
	globalCreds.wait()
	if epg.needsRefresh() {
		u, p := globalCreds.get()
		epg.refresh(u, p)
	}
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		if epg.needsRefresh() {
			u, p := globalCreds.get()
			epg.refresh(u, p)
		}
	}
}

// --- HTTP handlers ---

func handleAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	u := q.Get("username")
	p := q.Get("password")
	action := q.Get("action")

	if u != "" && p != "" {
		globalCreds.capture(u, p)
	}

	buildURL := func(act string) string {
		base := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=%s", upstream, u, p, act)
		for k, vs := range q {
			if k == "username" || k == "password" || k == "action" {
				continue
			}
			for _, v := range vs {
				base += "&" + k + "=" + v
			}
		}
		return base
	}

	fetchAndFilter := func(act string, filter func([]map[string]interface{}) []map[string]interface{}) {
		resp, err := apiClient.Get(buildURL(act))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		var items []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(filter(items))
	}

	switch action {
	case "get_live_streams":
		fetchAndFilter("get_live_streams", filterLive)
	case "get_vod_streams":
		fetchAndFilter("get_vod_streams", filterVod)
	case "get_series":
		fetchAndFilter("get_series", filterSeries)
	default:
		resp, err := apiClient.Get(buildURL(action))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		io.Copy(w, resp.Body)
	}
}

func handleEPG(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	u := q.Get("username")
	p := q.Get("password")
	if u != "" && p != "" {
		globalCreds.capture(u, p)
	}
	data := epg.get()
	if len(data) == 0 {
		http.Error(w, "EPG not ready", 503)
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.Write(data)
}

func handleStream(w http.ResponseWriter, r *http.Request) {
	target := upstream + r.URL.RequestURI()
	req, err := http.NewRequest(r.Method, target, nil)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := streamClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}

	// Track byte offset for reconnect (honoring initial Range seek if any)
	var offset int64
	if rng := r.Header.Get("Range"); rng != "" {
		fmt.Sscanf(strings.TrimPrefix(rng, "bytes="), "%d-", &offset)
	}

	// Forward headers — strip Content-Length so Kodi handles upstream drops as EOF
	for k, vs := range resp.Header {
		if strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Stream with automatic upstream reconnect on mid-stream drop (max 5 retries)
	for attempt := 0; attempt < 5; attempt++ {
		n, copyErr := io.Copy(w, resp.Body)
		resp.Body.Close()
		offset += n
		if copyErr == nil || n == 0 {
			return
		}
		log.Printf("Stream: upstream drop at %d bytes, reconnect %d/5", offset, attempt+1)
		retryReq, _ := http.NewRequest("GET", target, nil)
		retryReq.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		resp, err = streamClient.Do(retryReq)
		if err != nil {
			log.Printf("Stream: reconnect failed: %v", err)
			return
		}
	}
	resp.Body.Close()
}

func main() {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		log.SetOutput(f)
	}

	log.Printf("xtream-proxy starting on :%s", proxyPort)

	// Load credentials from disk
	if data, err := os.ReadFile(credsPath); err == nil {
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) >= 2 {
			u := strings.TrimSpace(lines[0])
			p := strings.TrimSpace(lines[1])
			globalCreds.mu.Lock()
			globalCreds.username = u
			globalCreds.password = p
			globalCreds.mu.Unlock()
			globalCreds.once.Do(func() { close(globalCreds.ready) })
			log.Printf("EPG: credentials chargés depuis disque")
		}
	}

	// Load codec cache
	codec.load()

	// Load EPG from disk
	epg.load()

	// Log EPG state
	log.Printf("EPG: credentials capturés, vérification du cache")
	if len(epg.data) == 0 {
		log.Printf("EPG: pas de cache disque, refresh au prochain tick")
	} else {
		log.Printf("EPG: cache disque %.1fh, refresh au prochain tick si >= 8h", epg.age().Hours())
	}

	go epgTicker()
	go codecScanner()

	mux := http.NewServeMux()
	mux.HandleFunc("/player_api.php", handleAPI)
	mux.HandleFunc("/xmltv.php", handleEPG)
	mux.HandleFunc("/", handleStream)

	log.Fatal(http.ListenAndServe(":"+proxyPort, mux))
}
