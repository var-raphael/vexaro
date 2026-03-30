package db

import (
	"database/sql"
	_ "modernc.org/sqlite"
)

func New(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// WAL mode — better concurrent read performance
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, err
	}

	if err := migrate(db); err != nil {
		return nil, err
	}

	return db, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		-- ---------------------------------------------------------------- serp --

		CREATE TABLE IF NOT EXISTS serp_cache (
			id         INTEGER PRIMARY KEY,
			user_id    TEXT NOT NULL,
			api_name   TEXT NOT NULL,
			intent     TEXT NOT NULL,
			query      TEXT NOT NULL,
			source     TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS serp_urls (
			id       INTEGER PRIMARY KEY,
			cache_id INTEGER NOT NULL REFERENCES serp_cache(id),
			url      TEXT NOT NULL,
			position INTEGER NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_serp_cache_intent    ON serp_cache(intent);
		CREATE INDEX IF NOT EXISTS idx_serp_cache_user_api  ON serp_cache(user_id, api_name);
		CREATE INDEX IF NOT EXISTS idx_serp_urls_cache_id   ON serp_urls(cache_id);

		-- --------------------------------------------------------------- crawl --

		CREATE TABLE IF NOT EXISTS crawl_urls (
			id         INTEGER PRIMARY KEY,
			user_id    TEXT NOT NULL,
			api_name   TEXT NOT NULL,
			url        TEXT NOT NULL,
			position   INTEGER NOT NULL,
			status     TEXT NOT NULL DEFAULT 'pending',
			retries    INTEGER NOT NULL DEFAULT 0,
			error      TEXT,
			crawled_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_crawl_urls_user_api ON crawl_urls(user_id, api_name);
		CREATE INDEX IF NOT EXISTS idx_crawl_urls_status   ON crawl_urls(status);
	`)
	return err
}
