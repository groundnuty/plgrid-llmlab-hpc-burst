package hubfiles

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// originKey is the canonical form of a URL's origin, "scheme://host[:port]":
// scheme and host in lower case, and the scheme's default port (443 for
// https, 80 for http) dropped, so "https://hub.example.org:443" and
// "https://hub.example.org" are one origin and "https://hub.example.org:8443"
// is another.
func originKey(u *url.URL) string {
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == defaultPort(scheme) {
		port = ""
	}
	switch {
	case port != "":
		host = net.JoinHostPort(host, port)
	case strings.Contains(host, ":"): // an IPv6 literal, without its brackets
		host = "[" + host + "]"
	}
	return scheme + "://" + host
}

func defaultPort(scheme string) string {
	switch scheme {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}

func sameOrigin(a, b *url.URL) bool { return originKey(a) == originKey(b) }

// withRedirectPolicy returns a copy of hc (or of a default client) whose
// redirect check runs the caller's own CheckRedirect first, honouring any
// error it returns, and then enforce on the request the caller's check has
// left. Running ours last matters: a callback may rebuild the request's
// headers (copying them from via[0], say), and net/http does no stripping
// after the callback, so only a check that comes after it can keep the token
// off another origin. Without a callback net/http's own limit of ten
// redirects applies.
func withRedirectPolicy(hc *http.Client, enforce func(req *http.Request) error) *http.Client {
	var out http.Client
	if hc != nil {
		out = *hc
	} else {
		out.Timeout = defaultTimeout
	}
	prev := out.CheckRedirect
	out.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if prev != nil {
			if err := prev(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("hubfiles: stopped after 10 redirects")
		}
		return enforce(req)
	}
	return &out
}

// listingRedirectPolicy is for the tree listing, whose answer is the
// authority on which files exist and is not verified afterwards: a redirect
// may stay on the Hub's origin (same scheme, host and port) and nowhere else,
// so the listing can never come from plain http or another host.
func listingRedirectPolicy(base *url.URL) func(*http.Request) error {
	return func(req *http.Request) error {
		if !sameOrigin(req.URL, base) {
			req.Header.Del("Authorization")
			return fmt.Errorf("%w: the listing was redirected to %s, expected %s", ErrCrossHostLink, originKey(req.URL), originKey(base))
		}
		return nil
	}
}

// downloadRedirectPolicy is for file downloads, whose bytes are verified
// against the listing's git blob id, so a redirect to a CDN is allowed. The
// token belongs to the Hub's origin only: net/http already drops it for
// another domain but keeps it for another port of the same host, and a
// caller's callback may restore it, so it is dropped here for every other
// origin.
func downloadRedirectPolicy(base *url.URL) func(*http.Request) error {
	return func(req *http.Request) error {
		if !sameOrigin(req.URL, base) {
			req.Header.Del("Authorization")
		}
		return nil
	}
}
