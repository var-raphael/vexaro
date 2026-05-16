package reddit

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

type QueuedRequest struct {
	DatasetID int64
	UserID    string
	DataName  string
	Cap       int
}

// RunFromQueue is called by the worker after queueRedditHandler has already
// inserted the dataset, subreddits, and URLs into the DB as 'pending'.
// It does NOT create a new dataset — it works entirely from what's in the DB.
func RunFromQueue(req QueuedRequest) error {
	if req.Cap <= 0 {
		req.Cap = 200
	}

	log.Printf("[reddit/queue] starting pipeline — dataset_id=%d", req.DatasetID)

	// load date range for this dataset
	var dateFrom, dateTo sql.NullString
	db.Get().QueryRow(`
		SELECT date_from, date_to FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&dateFrom, &dateTo)

	var fromTS, toTS float64
	layout := "2006-01-02"
	if dateFrom.Valid && dateFrom.String != "" {
		if t, err := time.Parse(layout, dateFrom.String); err == nil {
			fromTS = float64(t.Unix())
		}
	}
	if dateTo.Valid && dateTo.String != "" {
		if t, err := time.Parse(layout, dateTo.String); err == nil {
			toTS = float64(t.Unix())
		}
	}
	log.Printf("[reddit/queue] date range — from=%s to=%s", dateFrom.String, dateTo.String)

	// 1. Load all pending reddit_queue rows for this dataset
	type pendingRow struct {
		RedditQueueID int64
		DatasetURLID  int64
		URL           string
	}

	rows, err := db.Get().Query(`
		SELECT rq.reddit_queue_id, rq.dataset_url_id, du.url
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE du.dataset_id = ?
		  AND rq.status = 'pending'
	`, req.DatasetID)
	if err != nil {
		return fmt.Errorf("load pending rows: %w", err)
	}

	var pending []pendingRow
	for rows.Next() {
		var p pendingRow
		if err := rows.Scan(&p.RedditQueueID, &p.DatasetURLID, &p.URL); err != nil {
			rows.Close()
			return fmt.Errorf("scan pending: %w", err)
		}
		pending = append(pending, p)
	}
	rows.Close()

	if len(pending) == 0 {
		log.Printf("[reddit/queue] no pending rows for dataset_id=%d", req.DatasetID)
		return nil
	}

	log.Printf("[reddit/queue] found %d pending urls", len(pending))

	schema := DefaultSchema()
	saved := 0

	for _, p := range pending {
		db.Get().Exec(`
			UPDATE reddit_queue SET status = 'crawling' WHERE reddit_queue_id = ?
		`, p.RedditQueueID)

		if IsSubredditURL(p.URL) {
			var postURLs []string
			afterToken := ""

			for len(postURLs) < req.Cap {
				pageURL := p.URL
				if afterToken != "" {
					pageURL = fmt.Sprintf("%s&after=%s", p.URL, afterToken)
				}

				posts, nextAfter, dist, err := FetchSubredditPage(pageURL)
				if err != nil {
					log.Printf("[reddit/queue] fetch subreddit failed %s: %v", pageURL, err)
					break
				}
				if len(posts) == 0 {
					break
				}

				for _, post := range posts {
					if post.Permalink == "" {
						continue
					}
					// manual date filter — only needed for /new/.json
					if fromTS > 0 && post.CreatedUTC < fromTS {
						continue
					}
					if toTS > 0 && post.CreatedUTC > toTS {
						continue
					}
					postURLs = append(postURLs, "https://www.reddit.com"+post.Permalink)
				}

				log.Printf("[reddit/queue] paginated %s — %d posts so far", p.URL, len(postURLs))

				// if oldest post on this page is older than date_from stop paginating
				if fromTS > 0 && len(posts) > 0 {
					oldestPost := posts[len(posts)-1]
					if oldestPost.CreatedUTC < fromTS {
						log.Printf("[reddit/queue] reached date boundary — stopping pagination")
						break
					}
				}

				if nextAfter == "" {
					break
				}
				if dist < 100 {
					break
				}

				afterToken = nextAfter
			}

			// trim to cap
			if len(postURLs) > req.Cap {
				postURLs = postURLs[:req.Cap]
			}

			subSaved := 0
			for _, postURL := range postURLs {
				rawBytes, err := FetchRaw(postURL)
				if err != nil {
					log.Printf("[reddit/queue] fetch post failed %s: %v", postURL, err)
					continue
				}
				if err := SaveRawForExistingDataset(rawBytes, postURL, req.DatasetID, req.UserID, req.DataName, schema); err != nil {
					log.Printf("[reddit/queue] save post failed %s: %v", postURL, err)
					continue
				}
				subSaved++
				saved++
			}

			db.Get().Exec(`
				UPDATE reddit_queue SET status = 'done' WHERE reddit_queue_id = ?
			`, p.RedditQueueID)
			log.Printf("[reddit/queue] subreddit %s → %d/%d posts saved", p.URL, subSaved, len(postURLs))

		} else {
			// direct post URL — no date filtering needed, post is already specific
			rawBytes, err := FetchRaw(p.URL)
			if err != nil {
				log.Printf("[reddit/queue] fetch failed %s: %v", p.URL, err)
				db.Get().Exec(`
					UPDATE reddit_queue SET status = 'failed' WHERE reddit_queue_id = ?
				`, p.RedditQueueID)
				continue
			}
			if err := SaveRawForExistingDataset(rawBytes, p.URL, req.DatasetID, req.UserID, req.DataName, schema); err != nil {
				log.Printf("[reddit/queue] save failed %s: %v", p.URL, err)
				db.Get().Exec(`
					UPDATE reddit_queue SET status = 'failed' WHERE reddit_queue_id = ?
				`, p.RedditQueueID)
				continue
			}
			db.Get().Exec(`
				UPDATE reddit_queue SET status = 'done' WHERE reddit_queue_id = ?
			`, p.RedditQueueID)
			saved++
		}
	}

	log.Printf("[reddit/queue] fetch complete — %d saved", saved)

if saved == 0 {
    log.Printf("[reddit/queue] no posts saved — skipping combine and version")
    return nil
}

// 3. Combine — parse raw.json → filtered.json
log.Printf("[reddit/queue] starting combine")
if err := RunCombine(req.DatasetID); err != nil {
    return fmt.Errorf("combine: %w", err)
}

// 4. Version — assemble dataset.json + insert dataset_versions row
log.Printf("[reddit/queue] starting version")
if err := RunVersion(req.DatasetID); err != nil {
    return fmt.Errorf("version: %w", err)
}

log.Printf("[reddit/queue] pipeline complete — dataset_id=%d saved=%d", req.DatasetID, saved)
return nil
}