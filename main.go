package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/combine"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/format"
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
	http.HandleFunc("/test-format", testFormatHandler)
	http.HandleFunc("/test-extract", testExtractHandler)
	http.HandleFunc("/test-combine", testCombineHandler)

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

func testFormatHandler(w http.ResponseWriter, r *http.Request) {
	schema, err := format.ParseSchema(map[string]json.RawMessage{
		"name":          json.RawMessage(`"text"`),
		"description":   json.RawMessage(`"text"`),
		"origin":        json.RawMessage(`"text"`),
		"cat_img":       json.RawMessage(`"image"`),
		"cat_links":     json.RawMessage(`"link"`),
		"download_file": json.RawMessage(`"file"`),
	})
	if err != nil {
		log.Println("format schema error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := format.FormatFromPaths("clean.txt", schema); err != nil {
		log.Println("format error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("format complete")
	w.Write([]byte("done — format.json written alongside each cleaned.json\n"))
}


func testExtractHandler(w http.ResponseWriter, r *http.Request) {
	schema := map[string]*ai.SchemaField{
		"name": {
			Type:        "text",
			Description: "the name of the cat breed",
		},
		"description": {
			Type:        "text",
			Description: "a brief description of the cat breed's personality and appearance",
		},
		"origin": {
			Type:        "text",
			Description: "the country or region where the cat breed originated",
		},
		"cat_img": {
			Type:        "image",
			Description: "an image URL of the cat breed",
		},
		"cat_links": {
			Type:        "link",
			Description: "relevant links related to the cat breed",
		},
		"download_file": {
			Type:        "file",
			Description: "downloadable files related to the cat breed",
		},
	}

	if err := ai.ExtractFromPaths("format.txt", schema); err != nil {
		log.Println("extract error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("extract complete")
	w.Write([]byte("done — check extract.json\n"))
}

func testCombineHandler(w http.ResponseWriter, r *http.Request) {
	if err := combine.CombineFromPaths("extract.txt", "test-api"); err != nil {
		log.Println("combine error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("combine complete")
	w.Write([]byte("done — check data/test-api/result.json\n"))
}