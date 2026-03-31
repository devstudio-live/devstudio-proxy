package proxycore

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// CertDir returns (and creates) os.UserConfigDir()/devstudio-proxy/.
func CertDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "devstudio-proxy")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

// EnsureLocalCert loads certs from disk or generates new ones.
// fresh is true when the CA was newly generated (trust store install needed).
// Returns the leaf certPEM, keyPEM, the path to ca.crt, and the fresh flag.
func EnsureLocalCert() (certPEM, keyPEM []byte, caPath string, fresh bool, err error) {
	dir, err := CertDir()
	if err != nil {
		return nil, nil, "", false, err
	}

	caKeyPath  := filepath.Join(dir, "ca.key")
	caCertPath := filepath.Join(dir, "ca.crt")
	srvKeyPath := filepath.Join(dir, "server.key")
	srvCrtPath := filepath.Join(dir, "server.crt")
	caPath      = caCertPath

	caCertPEMBytes, caCertReadErr := os.ReadFile(caCertPath)
	caKeyPEMBytes, caKeyReadErr   := os.ReadFile(caKeyPath)

	var caKey *ecdsa.PrivateKey
	var caCert *x509.Certificate

	if caCertReadErr != nil || caKeyReadErr != nil {
		// Generate new CA — disk miss or first run.
		var newCAKeyPEM []byte
		caKey, caCert, caCertPEMBytes, newCAKeyPEM, err = generateCA()
		if err != nil {
			return nil, nil, "", false, err
		}
		if err = writeCertToDisk(caCertPath, caCertPEMBytes, 0644); err != nil {
			return nil, nil, "", false, err
		}
		if err = writeCertToDisk(caKeyPath, newCAKeyPEM, 0600); err != nil {
			return nil, nil, "", false, err
		}
		fresh = true
	} else {
		caKey, caCert, err = parseCAKeyPair(caCertPEMBytes, caKeyPEMBytes)
		if err != nil {
			// Corrupt files — regenerate.
			var newCAKeyPEM []byte
			caKey, caCert, caCertPEMBytes, newCAKeyPEM, err = generateCA()
			if err != nil {
				return nil, nil, "", false, err
			}
			if err = writeCertToDisk(caCertPath, caCertPEMBytes, 0644); err != nil {
				return nil, nil, "", false, err
			}
			if err = writeCertToDisk(caKeyPath, newCAKeyPEM, 0600); err != nil {
				return nil, nil, "", false, err
			}
			fresh = true
		}
	}

	srvCertPEMBytes, srvCertReadErr := os.ReadFile(srvCrtPath)
	srvKeyPEMBytes, srvKeyReadErr   := os.ReadFile(srvKeyPath)

	if srvCertReadErr != nil || srvKeyReadErr != nil || fresh || needsRegen(srvCertPEMBytes) {
		// Regenerate leaf cert (reuses existing CA — no new trust prompt needed).
		certPEM, keyPEM, err = generateLeafCert(caKey, caCert)
		if err != nil {
			return nil, nil, "", false, err
		}
		if err = writeCertToDisk(srvCrtPath, certPEM, 0644); err != nil {
			return nil, nil, "", false, err
		}
		if err = writeCertToDisk(srvKeyPath, keyPEM, 0600); err != nil {
			return nil, nil, "", false, err
		}
	} else {
		certPEM = srvCertPEMBytes
		keyPEM  = srvKeyPEMBytes
	}

	return certPEM, keyPEM, caPath, fresh, nil
}

// generateCA creates a new ECDSA P-256 CA certificate (10-year validity).
func generateCA() (key *ecdsa.PrivateKey, cert *x509.Certificate, certPEM, keyPEM []byte, err error) {
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "DevStudio Local CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return
	}

	cert, err = x509.ParseCertificate(certDER)
	if err != nil {
		return
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// generateLeafCert creates a localhost server cert signed by the given CA (2-year validity).
func generateLeafCert(caKey *ecdsa.PrivateKey, caCert *x509.Certificate) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(2 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// needsRegen returns true if the leaf cert expires within 30 days.
func needsRegen(certPEM []byte) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return true
	}
	return time.Until(cert.NotAfter) < 30*24*time.Hour
}

// writeCertToDisk writes data atomically using os.CreateTemp + os.Rename.
func writeCertToDisk(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-cert-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op after successful rename

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// parseCAKeyPair parses a PEM-encoded ECDSA CA cert + private key.
func parseCAKeyPair(certPEM, keyPEM []byte) (*ecdsa.PrivateKey, *x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("no certificate PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("no private key PEM block")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	// Sanity-check: verify the key pair is consistent.
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return nil, nil, err
	}

	return key, cert, nil
}
