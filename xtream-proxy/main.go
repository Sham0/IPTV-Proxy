package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func init() {
	// Android ne fournit pas /etc/resolv.conf aux binaires Linux — DNS forcé sur 8.8.8.8
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "udp", "192.168.0.254:53")
		},
	}
}

const (
	upstream   = "http://line.iptvhunt.com"
	listenPort = "8889"
	proxyHost  = "192.168.0.25"
	cacheTTL   = 4 * time.Hour
)

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

func keepLive(name string) bool {
	return strings.HasPrefix(name, "FR|") ||
		strings.HasPrefix(name, "24/7") ||
		strings.HasPrefix(name, "4K") ||
		name == "CA| FRENCH" ||
		name == "BE| WALLONIË" ||
		name == "EU| LUXEMBOURG"
}

func keepVod(name string) bool {
	return strings.HasPrefix(name, "|FR|") ||
		strings.HasPrefix(name, "|QC|") ||
		strings.HasPrefix(name, "|MULTI|") ||
		strings.HasPrefix(name, "4K") ||
		strings.Contains(name, "NETFLIX") ||
		strings.Contains(name, "APPLE+")
}

func fetchJSON(url string) (json.RawMessage, error) {
	resp, err := http.Get(url)
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

func catURL(r *http.Request, action string) string {
	u := r.URL.Query()
	return fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=%s",
		upstream, u.Get("username"), u.Get("password"), action)
}

func handleAPI(w http.ResponseWriter, r *http.Request) {
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
		w.Write(filterStreams(d, keptIDs(cats, keepLive)))

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
		w.Write(filterStreams(d, keptIDs(cats, keepVod)))

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
		w.Write(filterStreams(d, keptIDs(cats, keepVod)))

	default:
		// pass-through : get_vod_info, get_series_info, get_short_epg, etc.
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

func handleStream(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, upstream+r.URL.RequestURI(), http.StatusFound)
}

func main() {
	lf, _ := os.OpenFile("/sdcard/xtream-proxy.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetOutput(lf)
	log.Printf("xtream-proxy starting on :%s", listenPort)

	mux := http.NewServeMux()
	mux.HandleFunc("/player_api.php", handleAPI)
	mux.HandleFunc("/xmltv.php", handleStream)
	mux.HandleFunc("/", handleStream)

	if err := http.ListenAndServe(":"+listenPort, mux); err != nil {
		log.Fatal(err)
	}
}
