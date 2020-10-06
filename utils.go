package main

import (
	"crypto/x509"
	"fmt"
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
