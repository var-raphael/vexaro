package reddit

import (
	"fmt"
	"log"

	"github.com/var-raphael/vexaro-engine/db"
)

type TriggerRequest struct {
	DatasetID int64
	UserID    string
	DataName  string
	Cap       int
}

type TriggerResult struct {
	DatasetID  int64
	NewPosts   int
	Skipped    int
	Duplicates int
	Subreddits int
}

func Trigger(req TriggerRequest) (*TriggerResult, error) {
	if req.Cap <= 0 {
		req.Cap = 200
	}

	log.Printf("[reddit/trigger] starting — dataset_id=%d", req.DatasetID)

	// 1. Load subreddits for this dataset
	type subredditRow struct {
		SubredditID    int64
		Subreddit      string
		LastFetchedUTC *int64
	}

	rows, err := db.Get().Query(`
		SELECT subreddit_id, subreddit, last_fetched_utc
		FROM dataset_subreddits
		WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("load subreddits: %w", err)
	}

	var subreddits []subredditRow
	for rows.Next() {
		var s subredditRow
		if err := rows.Scan(&s.SubredditID, &s.Subreddit, &s.LastFetchedUTC); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan subreddit: %w", err)
		}
		subreddits = append(subreddits, s)
	}
	rows.Close()

	if len(subreddits) == 0 {
		log.Printf("[reddit/trigger] no subreddits for dataset_id=%d — skipping", req.DatasetID)
		return &TriggerResult{DatasetID: req.DatasetID}, nil
	}

	// 2. Load all existing URLs for this dataset — for dedup
	existingURLs, err := loadExistingURLs(req.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("load existing urls: %w", err)
	}
	log.Printf("[reddit/trigger] found %d existing urls in dataset", len(existingURLs))

	schema := DefaultSchema()
	result := &TriggerResult{
		DatasetID:  req.DatasetID,
		Subreddits: len(subreddits),
	}

	for _, sub := range subreddits {
		feedURL := fmt.Sprintf(
			"https://www.reddit.com/r/%s/new/.json?limit=%d&sort=new",
			sub.Subreddit, req.Cap,
		)

		log.Printf("[reddit/trigger] fetching r/%s (last_fetched_utc=%v)",
			sub.Subreddit, sub.LastFetchedUTC)

		posts, err := FetchSubreddit(feedURL)
		if err != nil {
			log.Printf("[reddit/trigger] fetch failed r/%s: %v", sub.Subreddit, err)
			continue
		}

		// 3. Filter by last_fetched_utc and dedup against existing URLs
		var newPosts []RedditPost
		var newestUTC int64

		for _, post := range posts {
			postUTC := int64(post.CreatedUTC)

			if postUTC > newestUTC {
				newestUTC = postUTC
			}

			// Skip if older than last fetch
			if sub.LastFetchedUTC != nil && postUTC <= *sub.LastFetchedUTC {
				result.Skipped++
				continue
			}

			// Build the post URL for dedup check
			if post.Permalink == "" {
				continue
			}
			postURL := "https://www.reddit.com" + post.Permalink

			// Skip if already in dataset
			if existingURLs[postURL] {
				result.Duplicates++
				log.Printf("[reddit/trigger] duplicate skipped: %s", postURL)
				continue
			}

			newPosts = append(newPosts, post)
		}

		if len(newPosts) == 0 {
			log.Printf("[reddit/trigger] no new posts for r/%s", sub.Subreddit)
			// Still update last_fetched_utc if we saw newer posts
			if newestUTC > 0 && (sub.LastFetchedUTC == nil || newestUTC > *sub.LastFetchedUTC) {
				updateLastFetchedUTC(sub.SubredditID, newestUTC)
			}
			continue
		}

		log.Printf("[reddit/trigger] r/%s — %d new posts (%d skipped, %d duplicates)",
			sub.Subreddit, len(newPosts), result.Skipped, result.Duplicates)

		// 4. Fetch full post + comments for each new post
		subSaved := 0
		for _, post := range newPosts {
			postURL := "https://www.reddit.com" + post.Permalink

			rawBytes, err := FetchRaw(postURL)
			if err != nil {
				log.Printf("[reddit/trigger] fetch post failed %s: %v", postURL, err)
				continue
			}

			if err := SaveRawForExistingDataset(
				rawBytes, postURL,
				req.DatasetID, req.UserID, req.DataName,
				schema,
			); err != nil {
				log.Printf("[reddit/trigger] save failed %s: %v", postURL, err)
				continue
			}

			// Add to existing URLs map to prevent same-run duplicates
			existingURLs[postURL] = true
			subSaved++
			result.NewPosts++
		}

		log.Printf("[reddit/trigger] r/%s — saved %d/%d posts",
			sub.Subreddit, subSaved, len(newPosts))

		// 5. Update last_fetched_utc to newest post seen
		if newestUTC > 0 && (sub.LastFetchedUTC == nil || newestUTC > *sub.LastFetchedUTC) {
			updateLastFetchedUTC(sub.SubredditID, newestUTC)
		}
	}

	if result.NewPosts == 0 {
		log.Printf("[reddit/trigger] no new posts found — skipping version")
		return result, nil
	}

	// 6. Combine — parse raw.json → filtered.json for new rows
	log.Printf("[reddit/trigger] running combine — dataset_id=%d", req.DatasetID)
	if err := RunCombine(req.DatasetID); err != nil {
		return result, fmt.Errorf("combine: %w", err)
	}

	// 7. Version — cumulative, all posts ever collected
	log.Printf("[reddit/trigger] running version — dataset_id=%d", req.DatasetID)
	if err := RunVersion(req.DatasetID); err != nil {
		return result, fmt.Errorf("version: %w", err)
	}

	log.Printf("[reddit/trigger] done — dataset_id=%d new_posts=%d skipped=%d duplicates=%d",
		req.DatasetID, result.NewPosts, result.Skipped, result.Duplicates)

	return result, nil
}

// loadExistingURLs returns a set of all URLs already in this dataset
func loadExistingURLs(datasetID int64) (map[string]bool, error) {
	rows, err := db.Get().Query(`
		SELECT url FROM datasets_url
		WHERE dataset_id = ? AND source_type = 'reddit'
	`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			continue
		}
		existing[u] = true
	}
	return existing, rows.Err()
}

func updateLastFetchedUTC(subredditID int64, utc int64) {
	_, err := db.Get().Exec(`
		UPDATE dataset_subreddits SET last_fetched_utc = ?
		WHERE subreddit_id = ?
	`, utc, subredditID)
	if err != nil {
		log.Printf("[reddit/trigger] update last_fetched_utc failed for subreddit_id=%d: %v",
			subredditID, err)
	} else {
		log.Printf("[reddit/trigger] updated last_fetched_utc for subreddit_id=%d → %d",
			subredditID, utc)
	}
}