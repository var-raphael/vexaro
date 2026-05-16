package crawl

// ProcessOne processes a single URL through the full layer cascade.
// Used by main.go for concurrent crawling.
func ProcessOne(cfg Config, apiName, url string, position int) error {
	cu := CrawlURL{URL: url, Position: position, Status: "pending"}
	return processURL(cfg, apiName, cu)
}
