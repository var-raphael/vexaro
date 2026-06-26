package reddit

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	reImage = regexp.MustCompile(`https?://[^\s)\]"]+\.(?:jpg|jpeg|png|gif|webp|svg|bmp|tiff?|ico|avif|heic|heif)(?:\?[^\s)\]"]*)?`)

	reVideo = regexp.MustCompile(`https?://[^\s)\]"]+\.(?:mp4|mov|avi|mkv|webm|flv|wmv|m4v|3gp|ogv|ts|mpg|mpeg)(?:\?[^\s)\]"]*)?`)

	reAudio = regexp.MustCompile(`https?://[^\s)\]"]+\.(?:mp3|wav|ogg|flac|aac|m4a|wma|opus|aiff|mid|midi)(?:\?[^\s)\]"]*)?`)

	reFile = regexp.MustCompile(`https?://[^\s)\]"]+\.(?:pdf|csv|zip|tar|gz|tar\.gz|tar\.bz2|rar|7z|docx?|xlsx?|pptx?|txt|json|xml|yaml|yml|md|epub|mobi|iso|dmg|exe|apk|deb|rpm|sh|py|js|ts|go|rs|cpp|c|h|java|kt|swift|sql|db|sqlite|log|torrent)(?:\?[^\s)\]"]*)?`)

	reURL = regexp.MustCompile(`https?://[^\s)\]"]+`)
)

var botAuthors = map[string]bool{
	"AutoModerator": true,
	"reddit":        true,
	"BotDefense":    true,
}

func isBot(author string) bool {
	if botAuthors[author] {
		return true
	}
	lower := strings.ToLower(author)
	return strings.HasSuffix(lower, "bot") || strings.HasSuffix(lower, "_bot")
}

func extractLinks(text string) (links, images, videos, audio, files []string) {
	all := reURL.FindAllString(text, -1)
	seen := map[string]bool{}
	for _, u := range all {
		if seen[u] {
			continue
		}
		seen[u] = true
		switch {
		case reFile.MatchString(u):
			files = append(files, u)
		case reVideo.MatchString(u):
			videos = append(videos, u)
		case reAudio.MatchString(u):
			audio = append(audio, u)
		case reImage.MatchString(u):
			images = append(images, u)
		default:
			links = append(links, u)
		}
	}
	return
}

func parseReplies(raw json.RawMessage) []RedditComment {
	if len(raw) == 0 || string(raw) == `""` {
		return nil
	}
	var listing RedditCommentListing
	if err := json.Unmarshal(raw, &listing); err != nil {
		return nil
	}
	var out []RedditComment
	for _, child := range listing.Data.Children {
		if child.Kind == "t1" {
			out = append(out, child.Data)
		}
	}
	return out
}

// ParseRaw takes raw Reddit JSON bytes and returns a FilteredPost.
// Handles both post+comment arrays ([PostListing, CommentListing])
// and single subreddit/search listings.
func ParseRaw(data []byte, schema RedditSchema) (*FilteredPost, error) {
	var listings []json.RawMessage
	if err := json.Unmarshal(data, &listings); err == nil && len(listings) >= 2 {
		return parsePostWithComments(listings[0], listings[1], schema)
	}
	return parseSingleListing(data, schema)
}

func parsePostWithComments(postRaw, commentsRaw json.RawMessage, schema RedditSchema) (*FilteredPost, error) {
	var postListing RedditListing
	if err := json.Unmarshal(postRaw, &postListing); err != nil {
		return nil, fmt.Errorf("parse post listing: %w", err)
	}

	var post *RedditPost
	for _, child := range postListing.Data.Children {
		if child.Kind == "t3" {
			p := child.Data
			post = &p
			break
		}
	}
	if post == nil {
		return nil, fmt.Errorf("no t3 post found in listing")
	}

	var commentListing RedditCommentListing
	var comments []RedditComment
	if err := json.Unmarshal(commentsRaw, &commentListing); err == nil {
		for _, child := range commentListing.Data.Children {
			if child.Kind == "t1" {
				comments = append(comments, child.Data)
			}
		}
	}

	out := ApplySchema(post, comments, schema)
	return &out, nil
}

func parseSingleListing(data []byte, schema RedditSchema) (*FilteredPost, error) {
	var listing RedditListing
	if err := json.Unmarshal(data, &listing); err != nil {
		return nil, fmt.Errorf("parse single listing: %w", err)
	}
	for _, child := range listing.Data.Children {
		if child.Kind == "t3" {
			p := child.Data
			out := ApplySchema(&p, nil, schema)
			return &out, nil
		}
	}
	return nil, fmt.Errorf("no parseable post found in listing")
}

func ApplySchema(post *RedditPost, comments []RedditComment, schema RedditSchema) FilteredPost {
	out := FilteredPost{FetchedAt: time.Now()}

	if schema.Title {
		out.Title = post.Title
	}
	if schema.Body && post.Selftext != "[deleted]" && post.Selftext != "[removed]" {
		out.Body = post.Selftext
	}
	if schema.Score {
		v := post.Score
		out.Score = &v
	}
	if schema.UpvoteRatio {
		v := post.UpvoteRatio
		out.UpvoteRatio = &v
	}
	if schema.NumComments {
		v := post.NumComments
		out.NumComments = &v
	}
	if schema.Subreddit {
		out.Subreddit = post.Subreddit
	}
	if schema.Flair {
		out.Flair = post.LinkFlairText
	}
	if schema.URL {
		out.URL = post.URL
	}
	if schema.CreatedUTC {
		v := post.CreatedUTC
		out.CreatedUTC = &v
	}
	if schema.Media {
		out.Media = post.Media
	}

	bodyClean := post.Selftext != "[deleted]" && post.Selftext != "[removed]"
	if (schema.Links || schema.Images || schema.Videos || schema.Audio || schema.Files) && bodyClean {
		l, i, v, a, f := extractLinks(post.Selftext)
		if schema.Links {
			out.Links = l
		}
		if schema.Images {
			out.Images = i
		}
		if schema.Videos {
			out.Videos = v
		}
		if schema.Audio {
			out.Audio = a
		}
		if schema.Files {
			out.Files = f
		}
	}

	if schema.Comments && len(comments) > 0 {
		out.Comments = filterComments(comments, schema)
	}
	return out
}

func filterComments(comments []RedditComment, schema RedditSchema) []FilteredComment {
	var out []FilteredComment
	for _, c := range comments {
		if c.Body == "[deleted]" || c.Body == "[removed]" || c.Body == "" {
			continue
		}
		if isBot(c.Author) {
			continue
		}

		fc := FilteredComment{}
		fc.Body = c.Body

		if schema.Score {
			v := c.Score
			fc.Score = &v
		}
		if schema.CreatedUTC {
			v := c.CreatedUTC
			fc.CreatedUTC = &v
		}
		if schema.Links || schema.Images || schema.Videos || schema.Audio {
			l, i, v, a, _ := extractLinks(c.Body)
			if schema.Links {
				fc.Links = l
			}
			if schema.Images {
				fc.Images = i
			}
			if schema.Videos {
				fc.Videos = v
			}
			if schema.Audio {
				fc.Audio = a
			}
		}

		replies := parseReplies(c.Replies)
		if len(replies) > 0 {
			fc.Replies = filterComments(replies, schema)
		}
		out = append(out, fc)
	}
	return out
}