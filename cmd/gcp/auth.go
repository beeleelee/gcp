package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kevinburke/ssh_config"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// sessionTTL is how long a session token remains valid after issue.
const sessionTTL = 5 * time.Minute

// challengeLen is the byte length of a random challenge sent to the client.
const challengeLen = 32

// sshConfigEntry holds resolved SSH config fields for a host alias.
type sshConfigEntry struct {
	Host         string // alias from the address (e.g. "myserver")
	HostName     string // resolved dial target (e.g. "192.168.1.100")
	User         string // from SSH config, or empty
	IdentityFile string // from SSH config, or empty
}

// sshConfigLookup looks up host in ~/.ssh/config and returns any matching
// Host entry. Returns nil if the file is missing or unreadable.
func sshConfigLookup(host string) *sshConfigEntry {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	f, err := os.Open(filepath.Join(home, ".ssh", "config"))
	if err != nil {
		return nil
	}
	defer f.Close()

	cfg, err := ssh_config.Decode(f)
	if err != nil {
		return nil
	}

	entry := &sshConfigEntry{Host: host}

	if v, err := cfg.Get(host, "HostName"); err == nil && v != "" {
		entry.HostName = v
	}
	if v, err := cfg.Get(host, "User"); err == nil && v != "" {
		entry.User = v
	}
	if v, err := cfg.Get(host, "IdentityFile"); err == nil && v != "" {
		// Expand ~ and relative paths relative to ~/.ssh
		expanded := os.ExpandEnv(v)
		if strings.HasPrefix(expanded, "~/") {
			expanded = filepath.Join(home, expanded[2:])
		} else if !filepath.IsAbs(expanded) {
			expanded = filepath.Join(home, ".ssh", expanded)
		}
		entry.IdentityFile = expanded
	}

	return entry
}

// sessionInfo holds the authenticated user identity, stored in the session
// store and in the gnet connection context after successful auth.
type sessionInfo struct {
	User           string
	Home           string
	EncryptionKey *[32]byte // derived from the SSH public key during auth
}

// sessionStore is a goroutine-safe map of session tokens to user info.
type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*sessionInfo
}

// newSessionStore creates an empty session store.
func newSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]*sessionInfo)}
}

// Put inserts a session token for the given user. It generates a random
// hex token internally and returns it along with the stored info.
func (s *sessionStore) Put(user, home string) (string, *sessionInfo) {
	token := sessionTokenHex()
	info := &sessionInfo{User: user, Home: home}
	s.mu.Lock()
	s.sessions[token] = info
	s.mu.Unlock()
	return token, info
}

// Get returns the session info for a token, or nil if not found.
func (s *sessionStore) Get(token string) *sessionInfo {
	s.mu.RLock()
	info := s.sessions[token]
	s.mu.RUnlock()
	return info
}

// Delete removes a session token.
func (s *sessionStore) Delete(token string) {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
}

// sessionTokenHex generates a random 32-byte hex-encoded session token.
func sessionTokenHex() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// generateChallenge returns a random challenge of challengeLen bytes.
func generateChallenge() ([]byte, error) {
	b := make([]byte, challengeLen)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generate challenge: %w", err)
	}
	return b, nil
}

// clientSigner attempts to find an SSH signer for the client. If identityFile
// is non-empty, only that specific key file is tried. Otherwise it tries the
// SSH agent first, then falls back to common private key paths.
func clientSigner(identityFile string) (ssh.Signer, ssh.PublicKey, error) {
	if identityFile != "" {
		return privateKeyFileSigner(identityFile)
	}
	signer, pub, err := sshAgentSigner()
	if err == nil {
		return signer, pub, nil
	}
	signer, pub, err = privateKeySigner()
	if err == nil {
		return signer, pub, nil
	}
	return nil, nil, fmt.Errorf("no SSH key available (try ssh-agent or add a key to ~/.ssh/id_{ed25519,rsa,ecdsa})")
}

// sshAgentSigner returns a signer from the SSH agent.
func sshAgentSigner() (ssh.Signer, ssh.PublicKey, error) {
	socket := os.Getenv("SSH_AUTH_SOCK")
	if socket == "" {
		return nil, nil, fmt.Errorf("SSH_AUTH_SOCK not set")
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh-agent dial: %w", err)
	}
	ac := agent.NewClient(conn)
	signers, err := ac.Signers()
	if err != nil || len(signers) == 0 {
		return nil, nil, fmt.Errorf("no keys in ssh-agent")
	}
	return signers[0], signers[0].PublicKey(), nil
}

// privateKeySigner tries common SSH private key paths and returns the
// first readable key. Passphrase-protected keys are skipped.
func privateKeySigner() (ssh.Signer, ssh.PublicKey, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	candidates := []string{
		filepath.Join(home, ".ssh", "id_ed25519"),
		filepath.Join(home, ".ssh", "id_rsa"),
		filepath.Join(home, ".ssh", "id_ecdsa"),
	}
	for _, p := range candidates {
		keyBytes, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		signer, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			continue
		}
		return signer, signer.PublicKey(), nil
	}
	return nil, nil, fmt.Errorf("no private key found in %s/.ssh", home)
}

// privateKeyFileSigner reads and parses a single SSH private key file.
// Passphrase-protected keys are skipped.
func privateKeyFileSigner(path string) (ssh.Signer, ssh.PublicKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read identity file %q: %w", path, err)
	}
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse identity file %q: %w", path, err)
	}
	return signer, signer.PublicKey(), nil
}

// verifySSHSignature parses the public key, unmarshals the SSH signature,
// and verifies it against challenge. On success it returns the parsed
// PublicKey for further matching against authorized keys.
func verifySSHSignature(pubKeyBytes, challenge, sigBytes []byte) (ssh.PublicKey, error) {
	pubKey, err := ssh.ParsePublicKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	var sig ssh.Signature
	if err := ssh.Unmarshal(sigBytes, &sig); err != nil {
		return nil, fmt.Errorf("unmarshal signature: %w", err)
	}
	if err := pubKey.Verify(challenge, &sig); err != nil {
		return nil, fmt.Errorf("verify signature: %w", err)
	}
	return pubKey, nil
}

// findUserByPubKey finds the OS user whose ~/.ssh/authorized_keys contains
// the given targetKey. If hintUser is non-empty, only that user is checked
// (skipping the full /etc/passwd scan). Returns the username and home
// directory on match.
func findUserByPubKey(targetKey ssh.PublicKey, hintUser string) (string, string, error) {
	targetBytes := targetKey.Marshal()

	if hintUser != "" {
		home, err := lookupHome(hintUser)
		if err != nil {
			return "", "", fmt.Errorf("hint user %q not found: %w", hintUser, err)
		}
		akPath := filepath.Join(home, ".ssh", "authorized_keys")
		ak, err := os.Open(akPath)
		if err != nil {
			return "", "", fmt.Errorf("no authorized_keys for user %q", hintUser)
		}
		defer ak.Close()
		akSc := bufio.NewScanner(ak)
		for akSc.Scan() {
			line := strings.TrimSpace(akSc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
			if err != nil {
				continue
			}
			if bytes.Equal(pubKey.Marshal(), targetBytes) {
				return hintUser, home, nil
			}
		}
		return "", "", fmt.Errorf("no matching key in %s's authorized_keys", hintUser)
	}

	type passwdEntry struct {
		Name string
		Home string
	}

	var entries []passwdEntry
	f, err := os.Open("/etc/passwd")
	if err != nil {
		return "", "", fmt.Errorf("open /etc/passwd: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ":")
		if len(parts) < 7 {
			continue
		}
		entries = append(entries, passwdEntry{Name: parts[0], Home: parts[5]})
	}

	for _, e := range entries {
		akPath := filepath.Join(e.Home, ".ssh", "authorized_keys")
		ak, err := os.Open(akPath)
		if err != nil {
			continue
		}
		akSc := bufio.NewScanner(ak)
		for akSc.Scan() {
			line := strings.TrimSpace(akSc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
			if err != nil {
				continue
			}
			if bytes.Equal(pubKey.Marshal(), targetBytes) {
				ak.Close()
				return e.Name, e.Home, nil
			}
		}
		ak.Close()
	}

	return "", "", fmt.Errorf("no matching user found for the provided SSH key")
}

// jailPath resolves reqPath relative to userHome and ensures the result
// stays within the home directory. It follows symlinks and checks the
// final resolved path against the home prefix.
func jailPath(userHome, reqPath string) (string, error) {
	cleanHome := filepath.Clean(userHome)
	jailed := filepath.Clean(filepath.Join(cleanHome, reqPath))

	// Quick prefix check before resolving symlinks.
	if !strings.HasPrefix(jailed, cleanHome+string(filepath.Separator)) && jailed != cleanHome {
		return "", fmt.Errorf("path %q escapes sandbox", reqPath)
	}

	// Resolve symlinks for the final check.
	real, err := filepath.EvalSymlinks(jailed)
	if err != nil {
		// Path doesn't exist yet (e.g. CreateReq) — use the cleaned path.
		real = jailed
	}
	if !strings.HasPrefix(real, cleanHome+string(filepath.Separator)) && real != cleanHome {
		return "", fmt.Errorf("symlink in path %q escapes sandbox", reqPath)
	}

	return jailed, nil
}

// hashPubKey returns a hex SHA-256 of a marshalled public key for use as
// a lookup key when we need to track in-flight challenges per pubkey.
func hashPubKey(pubKeyBytes []byte) string {
	h := sha256.Sum256(pubKeyBytes)
	return hex.EncodeToString(h[:])
}

// lookupHome returns the home directory for a given username by parsing
// /etc/passwd (avoids the overhead of os/user.Lookup which may use NSS).
func lookupHome(username string) (string, error) {
	f, err := os.Open("/etc/passwd")
	if err != nil {
		// fallback
		return lookupHomeNSS(username)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ":")
		if len(parts) >= 7 && parts[0] == username {
			return parts[5], nil
		}
	}
	return lookupHomeNSS(username)
}

func lookupHomeNSS(username string) (string, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return "", err
	}
	return u.HomeDir, nil
}

// deriveEncryptionKey derives a 32-byte symmetric encryption key from an SSH
// public key by SHA-256 hashing its authorized_keys format. Both client and
// server derive the same key from the same public key, so no key exchange is
// needed.
func deriveEncryptionKey(pubKey ssh.PublicKey) *[32]byte {
	marshaled := ssh.MarshalAuthorizedKey(pubKey)
	hash := sha256.Sum256(marshaled)
	key := new([32]byte)
	copy(key[:], hash[:])
	return key
}
