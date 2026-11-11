package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testServer() *server {
	return &server{cfg: config{SessionSecret: []byte("0123456789abcdef0123456789abcdef"), SessionTTL: time.Hour}}
}

func TestSessionRoundTripAndTampering(t *testing.T) {
	s := testServer()
	ss := session{UserID: "u1", Expires: time.Now().Add(time.Hour).Unix(), Nonce: "n1"}
	v := s.encodeSession(ss)
	got, ok := s.decodeSession(v)
	if !ok || got != ss {
		t.Fatalf("round trip: %+v %v", got, ok)
	}
	// Change the payload (e.g. to another user ID) without re-signing.
	other := s.encodeSession(session{UserID: "admin", Expires: ss.Expires, Nonce: "n1"})
	forged := strings.Split(other, ".")[0] + "." + strings.Split(v, ".")[1]
	if _, ok := s.decodeSession(forged); ok {
		t.Fatal("forged session accepted")
	}
	if _, ok := s.decodeSession(s.encodeSession(session{UserID: "u1", Expires: time.Now().Add(-time.Minute).Unix()})); ok {
		t.Fatal("expired session accepted")
	}
	s2 := &server{cfg: config{SessionSecret: []byte("another-secret-another-secret!!!")}}
	if _, ok := s2.decodeSession(v); ok {
		t.Fatal("session signed with another secret accepted")
	}
	if s.csrfToken(ss) == s.csrfToken(session{Nonce: "n2"}) {
		t.Fatal("CSRF token does not depend on the session")
	}
}

func TestCookieFlags(t *testing.T) {
	s := testServer()
	rec := httptest.NewRecorder()
	s.setCookie(rec, "v", 60)
	c := rec.Result().Cookies()[0]
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" {
		t.Fatalf("cookie flags: %+v", c)
	}
}

func TestLoginLimiter(t *testing.T) {
	l := newLoginLimiter()
	allowed := 0
	for i := 0; i < 30; i++ {
		if l.allow("10.0.0.1") {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("%d attempts allowed in a burst, want 10", allowed)
	}
	if !l.allow("10.0.0.2") {
		t.Fatal("another address was limited")
	}
}

func TestTemplatesParse(t *testing.T) {
	pages, err := loadTemplates()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"dashboard.html", "users.html", "user.html", "peers.html", "peer.html", "files.html",
		"file.html", "repairs.html", "audits.html", "billing.html", "settings.html", "log.html", "login.html"} {
		if pages[p] == nil {
			t.Errorf("missing page %s", p)
		}
	}
}
