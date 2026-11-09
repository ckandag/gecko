package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

type fakeBooleanFeatureFlagEvaluator struct {
	value bool  `json:"-"`
	err   error `json:"-"`
}

func (f fakeBooleanFeatureFlagEvaluator) Boolean(context.Context, string, bool) (bool, error) {
	return f.value, f.err
}

func TestConditionalAuthorizationMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		evaluator      fakeBooleanFeatureFlagEvaluator
		wantStatusCode int
		wantAuthzCalls int
	}{
		{
			name:           "enabled enforces authorization",
			evaluator:      fakeBooleanFeatureFlagEvaluator{value: true},
			wantStatusCode: http.StatusForbidden,
			wantAuthzCalls: 1,
		},
		{
			name:           "disabled bypasses authorization",
			evaluator:      fakeBooleanFeatureFlagEvaluator{value: false},
			wantStatusCode: http.StatusOK,
			wantAuthzCalls: 0,
		},
		{
			name:           "evaluation error enforces authorization",
			evaluator:      fakeBooleanFeatureFlagEvaluator{err: errors.New("provider unavailable")},
			wantStatusCode: http.StatusForbidden,
			wantAuthzCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authzCalls := 0
			authorization := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					authzCalls++
					w.WriteHeader(http.StatusForbidden)
				})
			}
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/apis/example/v1/namespaces/test/clusters", nil)
			conditionalAuthorizationMiddleware(tt.evaluator, authorization, logr.Discard())(next).ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatusCode {
				t.Fatalf("status code = %d, want %d", recorder.Code, tt.wantStatusCode)
			}
			if authzCalls != tt.wantAuthzCalls {
				t.Fatalf("authorization calls = %d, want %d", authzCalls, tt.wantAuthzCalls)
			}
		})
	}
}

func TestValidatePublicAuthAddress(t *testing.T) {
	tests := []struct {
		name          string
		enablePublic  bool
		address       string
		publicAddress string
		devAuth       bool
		disableAuth   bool
		wantErr       bool
	}{
		{
			name:         "auth enabled does not require loopback",
			enablePublic: true,
			address:      "0.0.0.0",
		},
		{
			name:          "dev auth loopback",
			enablePublic:  true,
			publicAddress: "127.0.0.1",
			devAuth:       true,
		},
		{
			name:          "dev auth localhost",
			enablePublic:  true,
			publicAddress: "localhost",
			devAuth:       true,
		},
		{
			name:          "dev auth IPv6 loopback",
			enablePublic:  true,
			publicAddress: "::1",
			devAuth:       true,
		},
		{
			name:          "dev auth non-loopback",
			enablePublic:  true,
			publicAddress: "0.0.0.0",
			devAuth:       true,
			wantErr:       true,
		},
		{
			name:         "dev auth implicit non-loopback",
			enablePublic: true,
			address:      "0.0.0.0",
			devAuth:      true,
			wantErr:      true,
		},
		{
			name:          "disabled auth non-loopback",
			enablePublic:  true,
			publicAddress: "192.0.2.10",
			disableAuth:   true,
			wantErr:       true,
		},
		{
			name:          "public API disabled",
			enablePublic:  false,
			publicAddress: "0.0.0.0",
			devAuth:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePublicAuthAddress(tt.enablePublic, tt.address, tt.publicAddress, tt.devAuth, tt.disableAuth)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validatePublicAuthAddress() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				for _, address := range []string{tt.address, tt.publicAddress} {
					if address != "" && strings.Contains(err.Error(), address) {
						t.Errorf("validation error exposes bind address %q: %v", address, err)
					}
				}
			}
		})
	}
}
