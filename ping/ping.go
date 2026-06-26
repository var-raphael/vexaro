package ping

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/var-raphael/vexaro-engine/db"
)

func PingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// extract key from /ping/{key}
	key := strings.TrimPrefix(r.URL.Path, "/ping/")
	key = strings.TrimSpace(key)
	if key == "" {
		http.Error(w, "ping key is required", http.StatusBadRequest)
		return
	}

	// look up dataset by ping_key
	var datasetID int64
	var dataName string
	var isFrozen int
	err := db.Get().QueryRow(`
		SELECT dataset_id, data_name, is_frozen
		FROM datasets WHERE ping_key = ?
	`, key).Scan(&datasetID, &dataName, &isFrozen)
	if err == sql.ErrNoRows {
		http.Error(w, "invalid ping key", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if isFrozen == 1 {
		http.Error(w, "dataset is frozen", http.StatusBadRequest)
		return
	}

	// reset web queue rows to pending
	_, err = db.Get().Exec(`
		UPDATE queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		SET q.status = 'pending'
		WHERE du.dataset_id = ?
	`, datasetID)
	if err != nil {
		http.Error(w, "reset queue failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// reset reddit queue rows to pending
	_, err = db.Get().Exec(`
		UPDATE reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		SET rq.status = 'pending'
		WHERE du.dataset_id = ?
	`, datasetID)
	if err != nil {
		http.Error(w, "reset reddit queue failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[ping] dataset_id=%d name=%q queues reset to pending", datasetID, dataName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":           true,
		"dataset_id":   datasetID,
		"dataset_name": dataName,
		"status":       "queues reset to pending",
	})
}