package reddit

import (
	"encoding/json"
	"time"
)

const (
  redditUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
	commentDepth    = 10
	requestTimeout  = 15
	rateLimit       = 1
	burstDelay      = 500 * time.Millisecond
	maxRetries      = 3
	retryDelay      = 5 * time.Second
)

type RedditRequest struct {
	UserID    string `json:"user_id"`
	DatasetID int64  `json:"dataset_id"`
	DataName  string `json:"data_name"`
	Intent    string `json:"intent"`
	Limit     int    `json:"limit"`
}

type RedditResponse struct {
	DatasetID int64    `json:"dataset_id"`
	URLs      []string `json:"urls"`
	Total     int      `json:"total"`
}

type RedditSchema struct {
	Title       bool `json:"title"`
	Body        bool `json:"body"`
	Score       bool `json:"score"`
	UpvoteRatio bool `json:"upvote_ratio"`
	NumComments bool `json:"num_comments"`
	Subreddit   bool `json:"subreddit"`
	Flair       bool `json:"flair"`
	URL         bool `json:"url"`
	CreatedUTC  bool `json:"created_utc"`
	Media       bool `json:"media"`
	Links       bool `json:"links"`
	Images      bool `json:"images"`
	Videos      bool `json:"videos"`
	Audio       bool `json:"audio"`
	Files       bool `json:"files"`
	Comments    bool `json:"comments"`
}

func DefaultSchema() RedditSchema {
	return RedditSchema{
		Title: true, Body: true, Score: true,
		UpvoteRatio: true, NumComments: true,
		Subreddit: true, Flair: true, URL: true, CreatedUTC: true,
		Media: true, Links: true, Images: true, Videos: true,
		Audio: true, Files: true, Comments: true,
	}
}

type RedditChild struct {
	Kind string     `json:"kind"`
	Data RedditPost `json:"data"`
}

type RedditListing struct {
	Data struct {
		Children []RedditChild `json:"children"`
		After    string        `json:"after"`
		Before   string        `json:"before"`
		Dist     int           `json:"dist"`
	} `json:"data"`
}

type RedditCommentListing struct {
	Data struct {
		Children []struct {
			Kind string        `json:"kind"`
			Data RedditComment `json:"data"`
		} `json:"children"`
	} `json:"data"`
}

type RedditPost struct {
	Title         string  `json:"title"`
	DisplayName   string  `json:"display_name"`
	Selftext      string  `json:"selftext"`
	Author        string  `json:"author"`
	Score         int     `json:"score"`
	Ups           int     `json:"ups"`
	Downs         int     `json:"downs"`
	UpvoteRatio   float64 `json:"upvote_ratio"`
	NumComments   int     `json:"num_comments"`
	Subreddit     string  `json:"subreddit"`
	LinkFlairText string  `json:"link_flair_text"`
	URL           string  `json:"url"`
	Permalink     string  `json:"permalink"`
	CreatedUTC    float64 `json:"created_utc"`
	IsSelf        bool    `json:"is_self"`
	Media         any     `json:"media"`
}

type RedditComment struct {
	Author     string          `json:"author"`
	Body       string          `json:"body"`
	Score      int             `json:"score"`
	Ups        int             `json:"ups"`
	Downs      int             `json:"downs"`
	CreatedUTC float64         `json:"created_utc"`
	Replies    json.RawMessage `json:"replies"`
}

type FilteredPost struct {
	Title       string            `json:"title,omitempty"`
	Body        string            `json:"body,omitempty"`
	Score       *int              `json:"score,omitempty"`
	UpvoteRatio *float64          `json:"upvote_ratio,omitempty"`
	NumComments *int              `json:"num_comments,omitempty"`
	Subreddit   string            `json:"subreddit,omitempty"`
	Flair       string            `json:"flair,omitempty"`
	URL         string            `json:"url,omitempty"`
	CreatedUTC  *float64          `json:"created_utc,omitempty"`
	Media       any               `json:"media,omitempty"`
	Links       []string          `json:"links,omitempty"`
	Images      []string          `json:"images,omitempty"`
	Videos      []string          `json:"videos,omitempty"`
	Audio       []string          `json:"audio,omitempty"`
	Files       []string          `json:"files,omitempty"`
	Comments    []FilteredComment `json:"comments,omitempty"`
	FetchedAt   time.Time         `json:"fetched_at"`
}

type FilteredComment struct {
	Body       string            `json:"body,omitempty"`
	Score      *int              `json:"score,omitempty"`
	CreatedUTC *float64          `json:"created_utc,omitempty"`
	Links      []string          `json:"links,omitempty"`
	Images     []string          `json:"images,omitempty"`
	Videos     []string          `json:"videos,omitempty"`
	Audio      []string          `json:"audio,omitempty"`
	Replies    []FilteredComment `json:"replies,omitempty"`
}