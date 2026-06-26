package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/notification"
)

// ==================================================================== plans --

type Plan struct {
	ID               string
	Name             string
	Price            int // in naira/dollars whole units, not kobo/cents
	Datasets         int
	URLs             int
	SERP             int
	PingURL          bool
	Webhook          bool
	PaystackPlanCode string // empty for free plan
}

// basePlans holds the static, non-env-dependent parts of each plan.
// PaystackPlanCode is filled in at lookup time via getPlanCode(), since
// package-level vars are evaluated before main() calls godotenv.Load(),
// which would otherwise capture an empty string for any PAYSTACK_*_PLAN_CODE
// env var read here directly.
var basePlans = map[string]Plan{
	"free": {
		ID:       "free",
		Name:     "Free",
		Price:    0,
		Datasets: 1,
		URLs:     20,
		SERP:     10,
		PingURL:  false,
		Webhook:  false,
	},
	"starter": {
		ID:       "starter",
		Name:     "Starter",
		Price:    26000,
		Datasets: 5,
		URLs:     100,
		SERP:     40,
		PingURL:  true,
		Webhook:  true,
	},
}

// planCodeEnvVars maps a plan ID to the env var holding its Paystack plan code.
var planCodeEnvVars = map[string]string{
	"starter": "PAYSTACK_STARTER_PLAN_CODE",
}

// GetPlan returns the plan by ID, with PaystackPlanCode resolved fresh from
// the environment on every call (cheap — os.Getenv is just a map lookup
// after process env is populated, no syscall per call worth worrying about).
func GetPlan(planID string) Plan {
	p, ok := basePlans[planID]
	if !ok {
		p = basePlans["free"]
	}
	if envVar, ok := planCodeEnvVars[p.ID]; ok {
		p.PaystackPlanCode = os.Getenv(envVar)
	}
	return p
}

// Plans is kept for any code that ranges over plan IDs directly (e.g.
// billingPlansHandler). Use GetPlan(id) instead of indexing this map
// directly whenever you need PaystackPlanCode populated.
var Plans = basePlans

// ==================================================================== paystack client --

// paystackSecretKey reads PAYSTACK_SECRET_KEY fresh on every call instead of
// once at package-init time. A package-level `var paystackSecretKey =
// os.Getenv(...)` would evaluate before main()'s godotenv.Load() runs,
// permanently capturing an empty string and silently sending unauthenticated
// requests to Paystack on every call.
func paystackSecretKey() string {
	return os.Getenv("PAYSTACK_SECRET_KEY")
}

const paystackBaseURL = "https://api.paystack.co"

func paystackRequest(method, path string, body interface{}) (map[string]interface{}, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		reqBody = strings.NewReader(string(b))
	}

	req, err := http.NewRequest(method, paystackBaseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+paystackSecretKey())
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if status, ok := result["status"].(bool); !ok || !status {
		msg := "unknown error"
		if m, ok := result["message"].(string); ok {
			msg = m
		}
		return result, fmt.Errorf("paystack error: %s", msg)
	}

	return result, nil
}

// ==================================================================== helpers --

func generateReference(userID string) string {
	return fmt.Sprintf("vexaro_%s_%d", userID, time.Now().UnixNano())
}

func setUserPlan(userID, planID string, expiresAt *time.Time) error {
	var expires interface{}
	if expiresAt != nil {
		expires = expiresAt.Format("2006-01-02 15:04:05")
	}
	_, err := db.Get().Exec(`
		UPDATE user SET plan = ?, pending_plan = NULL, plan_expires_at = ?
		WHERE user_id = ?
	`, planID, expires, userID)
	return err
}

func setPendingDowngrade(userID string) error {
	_, err := db.Get().Exec(`
		UPDATE user SET pending_plan = 'free' WHERE user_id = ?
	`, userID)
	return err
}

func freezeExcessDatasets(userID string, allowedCount int) error {
	rows, err := db.Get().Query(`
		SELECT dataset_id FROM datasets
		WHERE user_id = ?
		ORDER BY created_at ASC
	`, userID)
	if err != nil {
		return fmt.Errorf("query datasets: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	if len(ids) <= allowedCount {
		return nil
	}

	toFreeze := ids[allowedCount:]
	for _, id := range toFreeze {
		_, err := db.Get().Exec(`
			UPDATE datasets SET is_frozen = 1, nightly = 0 WHERE dataset_id = ?
		`, id)
		if err != nil {
			log.Printf("[billing] freeze dataset_id=%d failed: %v", id, err)
		} else {
			log.Printf("[billing] froze dataset_id=%d due to plan downgrade", id)
		}
	}
	return nil
}

// ==================================================================== handlers --

func billingPlansHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type planOut struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Price    int    `json:"price"`
		Datasets int    `json:"datasets"`
		URLs     int    `json:"urls"`
		SERP     int    `json:"serp"`
		PingURL  bool   `json:"ping_url"`
		Webhook  bool   `json:"webhook"`
	}

	var out []planOut
	for _, id := range []string{"free", "starter"} {
		p := GetPlan(id)
		out = append(out, planOut{
			ID: p.ID, Name: p.Name, Price: p.Price,
			Datasets: p.Datasets, URLs: p.URLs, SERP: p.SERP,
			PingURL: p.PingURL, Webhook: p.Webhook,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"plans": out})
}

func billingSubscribeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		PlanID string `json:"plan_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	plan := GetPlan(req.PlanID)
	if plan.PaystackPlanCode == "" {
		http.Error(w, "invalid plan_id", http.StatusBadRequest)
		return
	}

	var email string
	err := db.Get().QueryRow(`SELECT email FROM user WHERE user_id = ?`, userID).Scan(&email)
	if err == sql.ErrNoRows {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	reference := generateReference(userID)

	result, err := paystackRequest("POST", "/transaction/initialize", map[string]interface{}{
		"email":        email,
		"amount":       plan.Price * 100,
		"plan":         plan.PaystackPlanCode,
		"reference":    reference,
		"callback_url": os.Getenv("PAYSTACK_CALLBACK_URL"),
		"channels":     []string{"card"},
		"metadata": map[string]interface{}{
			"user_id": userID,
			"plan_id": plan.ID,
		},
	})
	if err != nil {
		log.Printf("[billing] initialize failed user=%s plan=%s: %v", userID, plan.ID, err)
		http.Error(w, "failed to initialize payment", http.StatusInternalServerError)
		return
	}

	data, _ := result["data"].(map[string]interface{})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":                true,
		"authorization_url": data["authorization_url"],
		"reference":         reference,
	})
}

func billingVerifyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	reference := strings.TrimSpace(r.URL.Query().Get("reference"))
	if reference == "" {
		http.Error(w, "reference is required", http.StatusBadRequest)
		return
	}

	result, err := paystackRequest("GET", "/transaction/verify/"+reference, nil)
	if err != nil {
		http.Error(w, "verification failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data, _ := result["data"].(map[string]interface{})
	status, _ := data["status"].(string)

	if status != "success" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "status": status})
		return
	}

	metadata, _ := data["metadata"].(map[string]interface{})
	metaUserID, _ := metadata["user_id"].(string)
	planID, _ := metadata["plan_id"].(string)

	if metaUserID == "" || planID == "" {
		http.Error(w, "missing metadata on transaction", http.StatusBadRequest)
		return
	}

	// Make sure the caller actually owns this transaction — without this check,
	// any authenticated user could pass another user's reference and have the
	// upgrade applied to their own account.
	if metaUserID != userID {
		log.Printf("[billing] verify mismatch — authed user=%s tried to verify reference=%s belonging to user=%s",
			userID, reference, metaUserID)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	expires := time.Now().AddDate(0, 1, 0)
	if err := setUserPlan(userID, planID, &expires); err != nil {
		http.Error(w, "failed to update plan: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[billing] user=%s upgraded to plan=%s via reference=%s", userID, planID, reference)

	notification.Notify(
		userID, nil, "plan_upgraded",
		fmt.Sprintf("You're now on the %s plan.", GetPlan(planID).Name),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "plan": planID})
}

func billingCancelHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if err := setPendingDowngrade(userID); err != nil {
		http.Error(w, "failed to cancel: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[billing] user=%s scheduled downgrade to free at period end", userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":      true,
		"message": "Your plan will switch to Free at the end of the current billing period.",
	})
}

func billingStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var plan string
	var pendingPlan sql.NullString
	var expiresAt sql.NullTime
	err := db.Get().QueryRow(`
		SELECT plan, pending_plan, plan_expires_at FROM user WHERE user_id = ?
	`, userID).Scan(&plan, &pendingPlan, &expiresAt)
	if err == sql.ErrNoRows {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	planDetails := GetPlan(plan)

	resp := map[string]interface{}{
		"plan":         plan,
		"plan_name":    planDetails.Name,
		"datasets":     planDetails.Datasets,
		"urls":         planDetails.URLs,
		"serp":         planDetails.SERP,
		"ping_url":     planDetails.PingURL,
		"webhook":      planDetails.Webhook,
		"pending_plan": nil,
		"expires_at":   nil,
	}
	if pendingPlan.Valid {
		resp["pending_plan"] = pendingPlan.String
	}
	if expiresAt.Valid {
		resp["expires_at"] = expiresAt.Time
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ==================================================================== webhook --

func verifyPaystackSignature(body []byte, signature string) bool {
	mac := hmac.New(sha512.New, []byte(paystackSecretKey()))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

func billingWebhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	signature := r.Header.Get("x-paystack-signature")
	if !verifyPaystackSignature(body, signature) {
		log.Printf("[billing/webhook] invalid signature — rejecting")
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var event struct {
		Event string `json:"event"`
		Data  struct {
			Reference string `json:"reference"`
			Status    string `json:"status"`
			Metadata  struct {
				UserID string `json:"user_id"`
				PlanID string `json:"plan_id"`
			} `json:"metadata"`
			Customer struct {
				Email string `json:"email"`
			} `json:"customer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	log.Printf("[billing/webhook] received event=%s reference=%s", event.Event, event.Data.Reference)

	switch event.Event {
	case "charge.success":
		userID := event.Data.Metadata.UserID
		planID := event.Data.Metadata.PlanID
		if userID == "" || planID == "" {
			log.Printf("[billing/webhook] charge.success missing metadata — skipping")
			break
		}
		expires := time.Now().AddDate(0, 1, 0)
		if err := setUserPlan(userID, planID, &expires); err != nil {
			log.Printf("[billing/webhook] failed to set plan user=%s: %v", userID, err)
			break
		}
		log.Printf("[billing/webhook] user=%s confirmed on plan=%s", userID, planID)

	case "invoice.payment_failed":
		log.Printf("[billing/webhook] payment failed reference=%s — will fall back to free at expiry", event.Data.Reference)

	case "subscription.disable":
		log.Printf("[billing/webhook] subscription disabled reference=%s", event.Data.Reference)
	}

	w.WriteHeader(http.StatusOK)
}

// ==================================================================== expiry sweep --

// RunBillingExpirySweep should be called periodically (e.g. from heal.Start())
func RunBillingExpirySweep() {
	rows, err := db.Get().Query(`
		SELECT user_id, pending_plan FROM user
		WHERE plan != 'free'
		AND plan_expires_at IS NOT NULL
		AND plan_expires_at <= NOW()
	`)
	if err != nil {
		log.Printf("[billing/sweep] query error: %v", err)
		return
	}

	type expiredUser struct {
		UserID      string
		PendingPlan sql.NullString
	}
	var expired []expiredUser
	for rows.Next() {
		var u expiredUser
		if rows.Scan(&u.UserID, &u.PendingPlan) == nil {
			expired = append(expired, u)
		}
	}
	rows.Close()

	for _, u := range expired {
		newPlan := "free"
		if u.PendingPlan.Valid && u.PendingPlan.String != "" {
			newPlan = u.PendingPlan.String
		}

		if err := setUserPlan(u.UserID, newPlan, nil); err != nil {
			log.Printf("[billing/sweep] failed to downgrade user=%s: %v", u.UserID, err)
			continue
		}

		allowed := GetPlan(newPlan).Datasets
		if err := freezeExcessDatasets(u.UserID, allowed); err != nil {
			log.Printf("[billing/sweep] failed to freeze datasets user=%s: %v", u.UserID, err)
		}

		log.Printf("[billing/sweep] user=%s downgraded to plan=%s, datasets capped at %d", u.UserID, newPlan, allowed)

		notification.Notify(
			u.UserID, nil, "plan_downgraded",
			"Your subscription ended and you've been moved to the Free plan.",
		)
	}

	if len(expired) > 0 {
		log.Printf("[billing/sweep] processed %d expired subscriptions", len(expired))
	}
}

// RunDatasetLimitSweep is a safety net that re-checks every user against
// their current plan's dataset limit and freezes any excess datasets.
// This covers edge cases the create-time gate can't catch (manual DB edits,
// migrations, race conditions, etc). Call this from the heal loop alongside
// RunBillingExpirySweep.
func RunDatasetLimitSweep() {
	rows, err := db.Get().Query(`SELECT user_id, plan FROM user`)
	if err != nil {
		log.Printf("[billing/dataset-sweep] query error: %v", err)
		return
	}

	type userPlan struct {
		UserID string
		Plan   string
	}
	var users []userPlan
	for rows.Next() {
		var u userPlan
		if rows.Scan(&u.UserID, &u.Plan) == nil {
			users = append(users, u)
		}
	}
	rows.Close()

	totalFrozen := 0
	for _, u := range users {
		allowed := GetPlan(u.Plan).Datasets
		before := CountUserDatasets(u.UserID)
		if before <= allowed {
			continue
		}
		if err := freezeExcessDatasets(u.UserID, allowed); err != nil {
			log.Printf("[billing/dataset-sweep] failed to freeze for user=%s: %v", u.UserID, err)
			continue
		}
		totalFrozen++
	}

	if totalFrozen > 0 {
		log.Printf("[billing/dataset-sweep] processed %d users with excess datasets", totalFrozen)
	}
}

// ==================================================================== gate helpers (use these in your existing handlers) --

func GetUserPlan(userID string) Plan {
	var planID string
	err := db.Get().QueryRow(`SELECT plan FROM user WHERE user_id = ?`, userID).Scan(&planID)
	if err != nil {
		return GetPlan("free")
	}
	return GetPlan(planID)
}

func CountUserDatasets(userID string) int {
	var count int
	db.Get().QueryRow(`SELECT COUNT(*) FROM datasets WHERE user_id = ?`, userID).Scan(&count)
	return count
}

func CountDatasetURLs(datasetID int64) int {
	var count int
	db.Get().QueryRow(`SELECT COUNT(*) FROM datasets_url WHERE dataset_id = ? AND source_type != 'discovery'`, datasetID).Scan(&count)
	return count
}

func CountDatasetSERPURLs(datasetID int64) int {
	var count int
	db.Get().QueryRow(`SELECT COUNT(*) FROM datasets_url WHERE dataset_id = ? AND source_type = 'serp'`, datasetID).Scan(&count)
	return count
}

// ==================================================================== route registration --

func RegisterBillingRoutes() {
	http.HandleFunc("/billing/plans", corsMiddleware(billingPlansHandler))
	http.HandleFunc("/billing/subscribe", corsMiddleware(authMiddleware(billingSubscribeHandler)))
	http.HandleFunc("/billing/verify", corsMiddleware(authMiddleware(billingVerifyHandler)))
	http.HandleFunc("/billing/cancel", corsMiddleware(authMiddleware(billingCancelHandler)))
	http.HandleFunc("/billing/status", corsMiddleware(authMiddleware(billingStatusHandler)))
	http.HandleFunc("/billing/webhook", billingWebhookHandler) // no CORS/auth — Paystack calls this server-to-server, verified by signature
}