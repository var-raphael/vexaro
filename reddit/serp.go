package reddit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	fhttp "github.com/Danny-Dasilva/fhttp"
)

const groqRetries = 3

func getRedditGroqKey() string {
	return os.Getenv("SERP_GROQ_KEYS")
}

func optimizeRedditQuery(intent string) string {
	key := getRedditGroqKey()
	if key == "" {
		log.Printf("[reddit/serp] no groq key — using raw intent as query")
		return intent
	}
	for attempt := 1; attempt <= groqRetries; attempt++ {
		query, err := tryOptimizeRedditQuery(intent, key)
		if err == nil && query != "" {
			log.Printf("[reddit/serp] groq optimized query: %q", query)
			return query
		}
		log.Printf("[reddit/serp] groq attempt %d/%d failed: %v", attempt, groqRetries, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	log.Printf("[reddit/serp] groq failed — falling back to raw intent")
	return intent
}

func tryOptimizeRedditQuery(intent, key string) (string, error) {
	prompt := fmt.Sprintf(
		"Convert the following user intent into a short Reddit search query.\n"+
			"Rules:\n"+
			"- Return ONLY 2-5 keywords, no more\n"+
			"- No site: operators, no quotes, no special characters\n"+
			"- Use the most specific technical terms relevant to the intent\n"+
			"- Plain keywords only, space separated\n"+
			"Intent: %s",
		intent,
	)

	body := map[string]any{
		"model":      "llama-3.1-8b-instant",
		"max_tokens": 30,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := fhttp.NewRequestWithContext(ctx, "POST",
		"https://api.groq.com/openai/v1/chat/completions",
		strings.NewReader(string(b)),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept-Encoding", "identity")

	client := &fhttp.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("bad groq response: %w", err)
	}
	if result.Error.Message != "" {
		return "", fmt.Errorf("groq error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("groq: no choices in response")
	}

	query := strings.TrimSpace(result.Choices[0].Message.Content)
	query = strings.Trim(query, `"'`)
	// strip any site: operators in case Groq still adds them
	if strings.Contains(query, "site:") {
		return "", fmt.Errorf("groq returned operator query — rejecting")
	}
	if query == "" {
		return "", fmt.Errorf("groq: empty query returned")
	}
	return query, nil
}


func isUsableRedditURL(u string) bool {
	parts := strings.Split(strings.TrimPrefix(u, "https://www.reddit.com"), "/")
	if len(parts) <= 3 && !strings.Contains(u, "/comments/") {
		return false
	}
	return true
}

func extractPostID(u string) string {
	parts := strings.Split(u, "/comments/")
	if len(parts) < 2 {
		return ""
	}
	return strings.SplitN(parts[1], "/", 2)[0]
}

func isTitleRelevant(title, intent string) bool {
	title = strings.ToLower(title)
	intent = strings.ToLower(intent)
	if strings.Contains(title, intent) {
		return true
	}
	tokens := strings.Fields(intent)
	if len(tokens) == 0 {
		return true
	}
	matched := 0
	for _, tok := range tokens {
		if strings.Contains(title, tok) {
			matched++
		}
	}
	if len(tokens) <= 3 {
		return matched == len(tokens)
	}
	return matched >= len(tokens)/2+1
}

func normalizeSlug(title string) string {
	title = strings.ToLower(title)
	var b strings.Builder
	for _, r := range title {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == ' ' {
			b.WriteRune(r)
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func discoverSubreddits(query string, maxSubs int) ([]string, error) {
	endpoint := fmt.Sprintf(
		"https://www.reddit.com/search.json?q=%s&type=sr&limit=10&sort=relevance",
		url.QueryEscape(query),
	)
	body, err := fetchRedditJSON(endpoint)
	if err != nil {
		return nil, fmt.Errorf("subreddit discovery: %w", err)
	}
	var listing RedditListing
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, fmt.Errorf("subreddit discovery parse: %w", err)
	}
	var subs []string
	for _, child := range listing.Data.Children {
		if child.Kind != "t5" {
			continue
		}
		name := child.Data.DisplayName
		if name == "" {
			name = child.Data.Title
		}
		if name != "" {
			subs = append(subs, name)
			if len(subs) >= maxSubs {
				break
			}
		}
	}
	if len(subs) == 0 {
		return nil, fmt.Errorf("no subreddits found for query: %q", query)
	}
	log.Printf("[reddit/serp] discovered subreddits: %v", subs)
	return subs, nil
}

func searchWithinSubreddits(query string, subreddits []string, limit int) ([]string, error) {
	fetchLimit := limit * 2
	if fetchLimit > 100 {
		fetchLimit = 100
	}
	multireddit := strings.Join(subreddits, "+")
	endpoint := fmt.Sprintf(
		"https://www.reddit.com/r/%s/search.json?q=%s&restrict_sr=true&sort=relevance&limit=%d",
		multireddit, url.QueryEscape(query), fetchLimit,
	)
	body, err := fetchRedditJSON(endpoint)
	if err != nil {
		return nil, fmt.Errorf("multireddit search: %w", err)
	}
	var listing RedditListing
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, fmt.Errorf("multireddit search parse: %w", err)
	}
	return extractPostURLs(listing, query), nil
}

func searchGlobal(query string, limit int) ([]string, error) {
	fetchLimit := limit * 2
	if fetchLimit > 100 {
		fetchLimit = 100
	}
	endpoint := fmt.Sprintf(
		"https://www.reddit.com/search.json?q=%s&type=link&sort=relevance&limit=%d",
		url.QueryEscape(query), fetchLimit,
	)
	body, err := fetchRedditJSON(endpoint)
	if err != nil {
		return nil, fmt.Errorf("global search: %w", err)
	}
	var listing RedditListing
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, fmt.Errorf("global search parse: %w", err)
	}
	return extractPostURLs(listing, query), nil
}

func extractPostURLs(listing RedditListing, intent string) []string {
	seenSlugs := map[string]bool{}
	var urls []string
	for _, child := range listing.Data.Children {
		if child.Kind != "t3" {
			continue
		}
		if strings.TrimSpace(child.Data.Title) == "" {
			log.Printf("[reddit/serp] skipping post with empty title: %s", child.Data.Permalink)
			continue
		}
		if !isTitleRelevant(child.Data.Title, intent) {
			log.Printf("[reddit/serp] skipping off-topic post: %q", child.Data.Title)
			continue
		}
		slug := normalizeSlug(child.Data.Title)
		if seenSlugs[slug] {
			log.Printf("[reddit/serp] skipping duplicate title: %q", child.Data.Title)
			continue
		}
		seenSlugs[slug] = true
		u := strings.TrimRight("https://www.reddit.com"+child.Data.Permalink, "/")
		urls = append(urls, u)
	}
	return urls
}

func SearchReddit(intent string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}

	// Step 1: optimize intent into best Reddit search query via Groq
	query := optimizeRedditQuery(intent)
	time.Sleep(time.Second)

	// Step 2: discover relevant subreddits using optimized query
	subreddits, err := discoverSubreddits(query, 5)
	if err != nil {
		log.Printf("[reddit/serp] subreddit discovery failed: %v", err)
		// fallback: if query is still too long (Groq unavailable), try first 3 words
		words := strings.Fields(query)
		if len(words) > 3 {
			shortQuery := strings.Join(words[:3], " ")
			log.Printf("[reddit/serp] retrying subreddit discovery with short query: %q", shortQuery)
			subreddits, err = discoverSubreddits(shortQuery, 5)
			if err != nil {
				log.Printf("[reddit/serp] short query discovery also failed: %v", err)
			}
		}
	}
	time.Sleep(time.Second)

	// Step 3: search posts within discovered subreddits
	var rawURLs []string
	if len(subreddits) > 0 {
		rawURLs, err = searchWithinSubreddits(query, subreddits, limit)
		if err != nil {
			log.Printf("[reddit/serp] multireddit search failed: %v, falling back to global", err)
		}
	}

	// Step 4: fallback to global search if multireddit yielded nothing
	if len(rawURLs) == 0 {
		log.Printf("[reddit/serp] falling back to global search")
		rawURLs, err = searchGlobal(query, limit)
		if err != nil {
			return nil, fmt.Errorf("global search failed: %w", err)
		}
	}
	time.Sleep(time.Second)

	// Step 5: dedup by URL and post ID
	seen := map[string]bool{}
	seenIDs := map[string]bool{}
	var urls []string
	for _, u := range rawURLs {
		if seen[u] || !isUsableRedditURL(u) {
			continue
		}
		postID := extractPostID(u)
		if postID != "" && seenIDs[postID] {
			log.Printf("[reddit/serp] skipping duplicate post id: %s", u)
			continue
		}
		if postID != "" {
			seenIDs[postID] = true
		}
		seen[u] = true
		urls = append(urls, u)
		if len(urls) >= limit {
			break
		}
	}

	if len(urls) == 0 {
		return nil, fmt.Errorf("reddit search returned no results for: %q", intent)
	}
	log.Printf("[reddit/serp] found %d urls for intent: %q → query: %q", len(urls), intent, query)
	return urls, nil
}