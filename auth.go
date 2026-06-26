package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey2 string

const authedUserIDKey contextKey2 = "authed_user_id"

const jwksURL = "https://jctoirrmiyjqznrtjqww.supabase.co/auth/v1/.well-known/jwks.json"

var jwks keyfunc.Keyfunc

// initAuth fetches and caches Supabase's public keys. Call this once
// at startup, before ListenAndServe.
func initAuth() error {
	k, err := keyfunc.NewDefaultCtx(context.Background(), []string{jwksURL})
	if err != nil {
		return fmt.Errorf("failed to load jwks: %w", err)
	}
	jwks = k
	return nil
}

// getAuthenticatedUserID verifies the Supabase JWT sent in the Authorization
// header and returns the trusted user_id (the "sub" claim). This is the only
// place user_id should ever come from.
func getAuthenticatedUserID(r *http.Request) (string, error) {
	tokenStr := extractBearerToken(r)
	if tokenStr == "" {
		return "", fmt.Errorf("no token provided")
	}

	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer("https://jctoirrmiyjqznrtjqww.supabase.co/auth/v1"),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}

	sub, ok := claims["sub"].(string)
	if !ok || sub == "" {
		return "", fmt.Errorf("token missing sub claim")
	}
	return sub, nil
}
// authMiddleware verifies the request and stores the trusted user_id in context.
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := getAuthenticatedUserID(r)
		if err != nil {
			http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), authedUserIDKey, userID)
		next(w, r.WithContext(ctx))
	}
}

// authedUserID pulls the verified user_id out of context.
func authedUserID(r *http.Request) (string, bool) {
	userID, ok := r.Context().Value(authedUserIDKey).(string)
	return userID, ok
}