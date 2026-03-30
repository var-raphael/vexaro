package ping

import (
	"encoding/json"
	"net/http"
)

// Response structure
type Response struct {
  Status string `json: "status"`
	Message string `json:"message"`
}

// PingHandler handles /ping requests
func PingHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := Response{
	  Status: "success",
		Message: "pong",
	}

	json.NewEncoder(w).Encode(response)
}