package reddit

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Proxy struct {
	URL       string
	Failures  int
	LastUsed  time.Time
	Dead      bool
}

type ProxyPool struct {
	mu      sync.Mutex
	proxies []*Proxy
}

var globalProxyPool = &ProxyPool{}

func (p *ProxyPool) Load(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.proxies = nil
	for _, u := range urls {
		if u == "" {
			continue
		}
		p.proxies = append(p.proxies, &Proxy{URL: u})
	}
	log.Printf("[proxy-pool] loaded %d proxies", len(p.proxies))
}

func (p *ProxyPool) LoadFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
    line := strings.TrimSpace(scanner.Text())
    if line == "" || strings.HasPrefix(line, "#") {
        continue
    }
    parts := strings.Split(line, ":")
    var proxyURL string
    if len(parts) == 4 {
        // ip:port:user:pass
        proxyURL = fmt.Sprintf("http://%s:%s@%s:%s", parts[2], parts[3], parts[0], parts[1])
    } else {
        if !strings.HasPrefix(line, "http") {
            line = "http://" + line
        }
        proxyURL = line
    }
    urls = append(urls, proxyURL)
}
	p.Load(urls)
	return scanner.Err()
}

func (p *ProxyPool) Pick() *Proxy {
	p.mu.Lock()
	defer p.mu.Unlock()

	var alive []*Proxy
	for _, px := range p.proxies {
		if !px.Dead {
			alive = append(alive, px)
		}
	}
	if len(alive) == 0 {
		log.Printf("[proxy-pool] all proxies dead — resurrecting")
		for _, px := range p.proxies {
			px.Dead = false
			px.Failures = 0
		}
		alive = p.proxies
	}
	if len(alive) == 0 {
		return nil
	}

	// Pick least recently used
	best := alive[0]
	for _, px := range alive[1:] {
		if px.LastUsed.Before(best.LastUsed) {
			best = px
		}
	}
	best.LastUsed = time.Now()
	return best
}

func (p *ProxyPool) MarkFailure(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, px := range p.proxies {
		if px.URL == proxyURL {
			px.Failures++
			if px.Failures >= 3 {
				px.Dead = true
				log.Printf("[proxy-pool] proxy retired after %d failures: %s", px.Failures, px.URL)
			}
			return
		}
	}
}

func (p *ProxyPool) MarkSuccess(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, px := range p.proxies {
		if px.URL == proxyURL {
			px.Failures = 0
			return
		}
	}
}

func (p *ProxyPool) Stats() (alive, dead int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, px := range p.proxies {
		if px.Dead {
			dead++
		} else {
			alive++
		}
	}
	return
}

func getClient(fallbackDirect bool) *http.Client {
	direct := &http.Client{Timeout: time.Duration(requestTimeout) * time.Second}

	if len(globalProxyPool.proxies) == 0 {
		return direct
	}

	if fallbackDirect {
		return direct
	}

	px := globalProxyPool.Pick()
	if px == nil {
		return direct
	}

	proxyURL, err := url.Parse(px.URL)
	if err != nil {
		return direct
	}

	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
		Timeout: time.Duration(requestTimeout) * time.Second,
	}
}

func LoadProxies(path string) error {
	return globalProxyPool.LoadFromFile(path)
}