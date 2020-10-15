package main

import (
	"bytes"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/minio/minio/pkg/console"
)

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

const (
	TLOG   = "LOG"
	TDEBUG = "DEBUG"
)

func logMsg(msg string) {
	if logFlag {
		fmt.Println(msg)
	}
}

// log debug statements
func logDMsg(msg string, err error) {
	if debugFlag {
		if err == nil {
			fmt.Println(msg)
			return
		}
		fmt.Println(msg, " :", err)
	}
}
func trace(rq *http.Request, rs *http.Response) string {
	var b = &strings.Builder{}

	fmt.Fprintf(b, "%s", console.Colorize("Request", "[REQUEST] "))
	fmt.Fprintf(b, "%s", console.Colorize("Method", fmt.Sprintf("%s %s", rq.Method, rq.URL.Path)))
	if rq.URL.RawQuery != "" {
		fmt.Fprintf(b, "?%s", rq.URL.RawQuery)
	}
	fmt.Fprint(b, "\n")
	hostStr := strings.Join(rq.Header["Host"], "")
	fmt.Fprintf(b, "%s", console.Colorize("Host", fmt.Sprintf("Host: %s\n", hostStr)))
	for k, v := range rq.Header {
		if k == "Host" {
			continue
		}
		fmt.Fprintf(b, "%s", console.Colorize("ReqHeaderKey",
			fmt.Sprintf("%s: ", k))+console.Colorize("HeaderValue", fmt.Sprintf("%s\n", strings.Join(v, ""))))
	}

	fmt.Fprintf(b, "%s", console.Colorize("Response", "[RESPONSE] "))
	if rs != nil {
		statusStr := console.Colorize("RespStatus", fmt.Sprintf("%d %s", rs.StatusCode, http.StatusText(rs.StatusCode)))
		if rs.StatusCode != http.StatusOK {
			statusStr = console.Colorize("ErrStatus", fmt.Sprintf("%d %s", rs.StatusCode, http.StatusText(rs.StatusCode)))
		}
		fmt.Fprintf(b, "%s\n", statusStr)

		for k, v := range rs.Header {
			fmt.Fprintf(b, "%s", console.Colorize("RespHeaderKey",
				fmt.Sprintf("%s: ", k))+console.Colorize("HeaderValue", fmt.Sprintf("%s\n", strings.Join(v, ""))))
		}
	}

	return b.String()
}
func migrateMsg(from, to string) string {
	return fmt.Sprintf("%s: Migrating %s => %s", console.Colorize("Request", "DryRun"), console.Colorize("Method", from), console.Colorize("Method", to))
}

// EncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func EncodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname strings.Builder
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname.WriteString("%" + strings.ToUpper(hex))
			}
		}
	}
	return encodedPathname.String()
}

// if object matches reserved string, no need to encode them
var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// Expects ascii encoded strings - from output of urlEncodePath
func percentEncodeSlash(s string) string {
	return strings.Replace(s, "/", "%2F", -1)
}

// QueryEncode - encodes query values in their URL encoded form. In
// addition to the percent encoding performed by urlEncodePath() used
// here, it also percent encodes '/' (forward slash)
func QueryEncode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := percentEncodeSlash(EncodePath(k)) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(percentEncodeSlash(EncodePath(v)))
		}
	}
	return buf.String()
}
