package crawl

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

type crawlProxy struct {
	URL      string
	Failures int
	LastUsed time.Time
	Dead     bool
}

type crawlProxyPool struct {
	mu      sync.Mutex
	proxies []*crawlProxy
}

var globalCrawlProxyPool = &crawlProxyPool{}

func LoadCrawlProxies(path string) error {
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
			proxyURL = fmt.Sprintf("http://%s:%s@%s:%s", parts[2], parts[3], parts[0], parts[1])
		} else {
			if !strings.HasPrefix(line, "http") {
				line = "http://" + line
			}
			proxyURL = line
		}
		urls = append(urls, proxyURL)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	globalCrawlProxyPool.mu.Lock()
	defer globalCrawlProxyPool.mu.Unlock()
	globalCrawlProxyPool.proxies = nil
	for _, u := range urls {
		if u == "" {
			continue
		}
		globalCrawlProxyPool.proxies = append(globalCrawlProxyPool.proxies, &crawlProxy{URL: u})
	}
	log.Printf("[crawl-proxy] loaded %d proxies", len(globalCrawlProxyPool.proxies))
	return nil
}

func (p *crawlProxyPool) pick() *crawlProxy {
	p.mu.Lock()
	defer p.mu.Unlock()

	var alive []*crawlProxy
	for _, px := range p.proxies {
		if !px.Dead {
			alive = append(alive, px)
		}
	}
	if len(alive) == 0 {
		for _, px := range p.proxies {
			px.Dead = false
			px.Failures = 0
		}
		alive = p.proxies
	}
	if len(alive) == 0 {
		return nil
	}

	// Shuffle to avoid always hitting the same proxy
	idx := rand.Intn(len(alive))
	alive[idx].LastUsed = time.Now()
	return alive[idx]
}

func (p *crawlProxyPool) markFailure(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, px := range p.proxies {
		if px.URL == proxyURL {
			px.Failures++
			if px.Failures >= 3 {
				px.Dead = true
				log.Printf("[crawl-proxy] proxy retired: %s", px.URL)
			}
			return
		}
	}
}

func (p *crawlProxyPool) markSuccess(proxyURL string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, px := range p.proxies {
		if px.URL == proxyURL {
			px.Failures = 0
			return
		}
	}
}