// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/nereidai/nereidedge/internal/config"
)

// loadGatewayTLSMaterials reads the optional PEM materials for the
// NereidLink MQTT connection. Each material can be supplied as inline PEM
// text (typically through an NEREIDEDGE_* environment override) or as a
// mounted file path; inline PEM wins when both are set. Empty inputs leave
// the corresponding PEM nil so the SDK falls back to the system trust store
// (server authentication) or id/secret authentication. The client
// certificate and key must be configured together through the same delivery
// mode; a missing or unreadable file fails fast before the gateway opens any
// connection.
func loadGatewayTLSMaterials(cfg config.NereidEdgeConfig) (rootPEM, certPEM, keyPEM []byte, err error) {
	if rootPEM, err = loadPEMMaterial("RootCA", cfg.RootCAPEM, cfg.RootCAFile); err != nil {
		return nil, nil, nil, err
	}
	// The cert/key pair must use one delivery mode: detect mixing from the
	// configured fields before reading anything, since non-empty results
	// alone cannot tell inline bytes from file contents.
	inlinePair := cfg.ClientCertPEM != "" || cfg.ClientKeyPEM != ""
	filePair := cfg.ClientCertFile != "" || cfg.ClientKeyFile != ""
	if inlinePair && filePair {
		return nil, nil, nil, errors.New(
			"NereidEdge client certificate and key must use one delivery mode: inline PEM or files, not both")
	}
	switch {
	case !inlinePair && !filePair:
		return rootPEM, nil, nil, nil
	case filePair && (cfg.ClientCertFile == "" || cfg.ClientKeyFile == ""):
		return nil, nil, nil, errors.New("NereidEdge.ClientCertFile and ClientKeyFile must be configured together")
	case inlinePair && (cfg.ClientCertPEM == "" || cfg.ClientKeyPEM == ""):
		return nil, nil, nil, errors.New("NereidEdge.ClientCertPEM and ClientKeyPEM must be configured together")
	}
	if certPEM, err = loadPEMMaterial("ClientCert", cfg.ClientCertPEM, cfg.ClientCertFile); err != nil {
		return nil, nil, nil, err
	}
	if keyPEM, err = loadPEMMaterial("ClientKey", cfg.ClientKeyPEM, cfg.ClientKeyFile); err != nil {
		return nil, nil, nil, err
	}
	return rootPEM, certPEM, keyPEM, nil
}

// loadPEMMaterial resolves one PEM material: non-empty inline text takes
// precedence, otherwise the file is read when a path is given.
func loadPEMMaterial(field, inlinePEM, filePath string) ([]byte, error) {
	if inlinePEM != "" {
		return []byte(normalizeInlinePEM(inlinePEM)), nil
	}
	if filePath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read NereidEdge.%sFile: %w", field, err)
	}
	return data, nil
}

// normalizeInlinePEM converts literal "\n" escapes into real line breaks so a
// PEM written as a single .env line is accepted. An already multi-line PEM
// passes through unchanged: the PEM base64 alphabet and its BEGIN/END labels
// never contain a backslash, so the replacement cannot corrupt real content.
func normalizeInlinePEM(s string) string {
	if !strings.Contains(s, `\n`) {
		return s
	}
	s = strings.ReplaceAll(s, `\r\n`, "\n")
	return strings.ReplaceAll(s, `\n`, "\n")
}
