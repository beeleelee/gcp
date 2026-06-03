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

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// sessionTTL is how long a session token remains valid after issue.
const sessionTTL = 5 * time.Minute

// challengeLen is the byte length of a random challenge sent to the client.
const challengeLen = 32

// sessionInfo holds the authenticated user identity, stored in the session
// store and in the gnet connection context after successful auth.
type sessionInfo struct {
	User string
	Home string
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

// clientSigner attempts to find an SSH signer for the client, trying the
// SSH agent first, then falling back to common private key paths.
func clientSigner() (ssh.Signer, ssh.PublicKey, error) {
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

// findUserByPubKey scans system users by reading /etc/passwd and checks
// each user's ~/.ssh/authorized_keys for a matching public key. Returns
// the username and home directory on the first match.
func findUserByPubKey(targetKey ssh.PublicKey) (string, string, error) {
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

	targetBytes := targetKey.Marshal()

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
