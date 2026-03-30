package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/ping"
	"github.com/var-raphael/vexaro-engine/serp"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[env] no .env file found — relying on system env")
	}

	http.HandleFunc("/ping", ping.PingHandler)
	http.HandleFunc("/test-serp", testSerpHandler)
	http.HandleFunc("/test-crawl", testCrawlHandler)
	http.HandleFunc("/test-clean", testCleanHandler)

	port := ":8080"
	log.Println("server running on", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}

func testSerpHandler(w http.ResponseWriter, r *http.Request) {
	res, err := serp.Fetch(nil, serp.SERPRequest{
		UserID:  "dev-user",
		APIName: "test-api",
		Intent:  "top cats pet breed",
		Limit:   2,
	})
	if err != nil {
		log.Println("serp error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	content := "Source: " + res.Source + "\n\n" + strings.Join(res.URLs, "\n")
	if err := os.WriteFile("serp_results.txt", []byte(content), 0644); err != nil {
		log.Println("write error:", err)
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}

	log.Println("serp results written to serp_results.txt")
	w.Write([]byte("done — check serp_results.txt\n"))
}

func testCrawlHandler(w http.ResponseWriter, r *http.Request) {
	cfg := crawl.Config{
		BrowserlessKey: os.Getenv("BROWSERLESS_KEYS"),
	}

	if err := crawl.CrawlFromFile(cfg, "test-api", "serp_results.txt"); err != nil {
		log.Println("crawl error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("crawl complete")
	w.Write([]byte("done — check data/test-api/\n"))
}

func testCleanHandler(w http.ResponseWriter, r *http.Request) {
	if err := clean.CleanFromPaths("paths.txt"); err != nil {
		log.Println("clean error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("clean complete")
	w.Write([]byte("done — cleaned.json written alongside each raw.json\n"))
}
