package format

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var sentenceBoundary = regexp.MustCompile(`(?:[.!?]["')]?\s+)`)

// chunkContent splits compressed content into chunks of up to maxWords words,
// always breaking at the nearest sentence boundary.
func chunkContent(content string, maxWords int) []string {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}

	// find all sentence boundary positions
	indices := sentenceBoundary.FindAllStringIndex(content, -1)
	if len(indices) == 0 {
		return []string{content}
	}

	// build a list of cut points — end position of each sentence
	var cutPoints []int
	for _, loc := range indices {
		cutPoints = append(cutPoints, loc[1])
	}
	// always include end of content as final cut point
	cutPoints = append(cutPoints, len(content))

	var chunks []string
	start := 0

	for start < len(content) {
		// find the furthest cut point where word count <= maxWords
		segment := ""
		chosenEnd := -1

		for _, end := range cutPoints {
			if end <= start {
				continue
			}
			candidate := strings.TrimSpace(content[start:end])
			wordCount := len(strings.Fields(candidate))
			if wordCount <= maxWords {
				segment = candidate
				chosenEnd = end
			} else {
				break
			}
		}

		if chosenEnd == -1 {
			// no cut point found within maxWords — force cut at maxWords
			words := strings.Fields(content[start:])
			if len(words) <= maxWords {
				chunks = append(chunks, strings.TrimSpace(content[start:]))
				break
			}
			forced := strings.Join(words[:maxWords], " ")
			chunks = append(chunks, forced)
			// advance start past the forced chunk
			start += len(forced)
			// skip any whitespace
			for start < len(content) && (content[start] == ' ' || content[start] == '\n') {
				start++
			}
			continue
		}

		if segment != "" {
			chunks = append(chunks, segment)
		}
		start = chosenEnd
	}

	return chunks
}

// writeChunks writes the first chunk to format.json and saves the rest
// as raw text in strip.txt in the same directory.
func writeChunks(dir string, chunks []string) error {
	if len(chunks) == 0 {
		return nil
	}

	// write strip.txt with remaining chunks as raw text
	if len(chunks) > 1 {
		remaining := strings.Join(chunks[1:], " ")
		stripPath := filepath.Join(dir, "strip.txt")
		if err := os.WriteFile(stripPath, []byte(remaining), 0644); err != nil {
			return fmt.Errorf("write strip.txt: %w", err)
		}
	}

	return nil
}

// loadStrip reads remaining content from strip.txt if it exists.
// Returns empty string if file doesn't exist.
func loadStrip(dir string) (string, error) {
	stripPath := filepath.Join(dir, "strip.txt")
	b, err := os.ReadFile(stripPath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read strip.txt: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// clearStrip deletes strip.txt once all chunks have been processed.
func clearStrip(dir string) error {
	stripPath := filepath.Join(dir, "strip.txt")
	err := os.Remove(stripPath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// nextChunk reads the next 6000-word chunk from strip.txt,
// updates strip.txt with the remainder, and returns the chunk.
// Returns empty string if strip.txt is empty or doesn't exist.
func nextChunk(dir string, maxWords int) (string, error) {
	remaining, err := loadStrip(dir)
	if err != nil {
		return "", err
	}
	if remaining == "" {
		return "", nil
	}

	chunks := chunkContent(remaining, maxWords)
	if len(chunks) == 0 {
		return "", nil
	}

	chunk := chunks[0]

	// update strip.txt with the rest
	if len(chunks) > 1 {
		rest := strings.Join(chunks[1:], " ")
		stripPath := filepath.Join(dir, "strip.txt")
		if err := os.WriteFile(stripPath, []byte(rest), 0644); err != nil {
			return "", fmt.Errorf("update strip.txt: %w", err)
		}
	} else {
		// no more content — clear strip.txt
		if err := clearStrip(dir); err != nil {
			return "", err
		}
	}

	return chunk, nil
}

// hasMoreChunks returns true if strip.txt exists and has content.
func hasMoreChunks(dir string) bool {
	stripPath := filepath.Join(dir, "strip.txt")
	info, err := os.Stat(stripPath)
	if err != nil {
		return false
	}
	return info.Size() > 0
}

// readLines reads lines from a file — local helper to avoid import cycle
// if chunk.go is ever moved. Uses bufio for large files.
func readLinesFromFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}
