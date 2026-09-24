// Copyright 2026 The smokeping_prober Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var (
	// ErrTimeout means a command did not finish before its deadline.
	ErrTimeout = errors.New("command timed out")
	// ErrClosed means the session is gone and must be replaced.
	ErrClosed = errors.New("session closed")
)

const (
	loginTimeout = 15 * time.Second
	setupTimeout = 10 * time.Second
	ctrlCTimeout = 3 * time.Second
)

// promptRe matches a VRP prompt at the end of the output: <name>, [name] or [~name].
var promptRe = regexp.MustCompile(`([<\[][~*]?[^\s<>\[\]]+[>\]])\s*$`)

// SSHConfig describes how to reach a router.
type SSHConfig struct {
	Address             string
	Username            string
	Password            string
	PrivateKey          []byte
	KnownHostsFile      string
	InsecureSkipHostKey bool
	DialTimeout         time.Duration
}

// Session is an interactive VRP shell that runs one command at a time.
type Session struct {
	client    *ssh.Client
	sess      *ssh.Session
	stdin     io.Writer
	chunks    chan []byte
	buf       []byte
	prompt    string
	closeOnce sync.Once
}

// Dial logs into the router, learns its prompt and disables paging.
func Dial(ctx context.Context, cfg SSHConfig) (*Session, error) {
	hostKey, err := hostKeyCallback(cfg)
	if err != nil {
		return nil, err
	}
	auth, err := authMethods(cfg)
	if err != nil {
		return nil, err
	}
	timeout := cfg.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Address, err)
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	c, chans, reqs, err := ssh.NewClientConn(conn, cfg.Address, &ssh.ClientConfig{
		User:            cfg.Username,
		Auth:            auth,
		HostKeyCallback: hostKey,
		Timeout:         timeout,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh handshake with %s: %w", cfg.Address, err)
	}
	_ = conn.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	s, err := startShell(client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

func hostKeyCallback(cfg SSHConfig) (ssh.HostKeyCallback, error) {
	if cfg.InsecureSkipHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	if cfg.KnownHostsFile == "" {
		return nil, errors.New("known_hosts file is required")
	}
	cb, err := knownhosts.New(cfg.KnownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("load known_hosts: %w", err)
	}
	return cb, nil
}

func authMethods(cfg SSHConfig) ([]ssh.AuthMethod, error) {
	var auth []ssh.AuthMethod
	if len(cfg.PrivateKey) > 0 {
		signer, err := ssh.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		auth = append(auth,
			ssh.Password(cfg.Password),
			ssh.KeyboardInteractive(func(_, _ string, questions []string, _ []bool) ([]string, error) {
				answers := make([]string, len(questions))
				for i := range answers {
					answers[i] = cfg.Password
				}
				return answers, nil
			}),
		)
	}
	if len(auth) == 0 {
		return nil, errors.New("no password or private key")
	}
	return auth, nil
}

func startShell(client *ssh.Client) (*Session, error) {
	sess, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}
	stdin, err := sess.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin: %w", err)
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout: %w", err)
	}
	modes := ssh.TerminalModes{ssh.ECHO: 1, ssh.TTY_OP_ISPEED: 115200, ssh.TTY_OP_OSPEED: 115200}
	// A wide terminal keeps long ping commands from wrapping in the echo.
	if err := sess.RequestPty("vt100", 200, 512, modes); err != nil {
		return nil, fmt.Errorf("request pty: %w", err)
	}
	if err := sess.Shell(); err != nil {
		return nil, fmt.Errorf("start shell: %w", err)
	}
	s := &Session{client: client, sess: sess, stdin: stdin, chunks: make(chan []byte, 64)}
	go s.readLoop(stdout)
	if err := s.learnPrompt(); err != nil {
		s.Close()
		return nil, err
	}
	if _, err := s.Run("screen-length 0 temporary", setupTimeout); err != nil {
		s.Close()
		return nil, fmt.Errorf("disable paging: %w", err)
	}
	return s, nil
}

func (s *Session) readLoop(r io.Reader) {
	defer close(s.chunks)
	for {
		b := make([]byte, 4096)
		n, err := r.Read(b)
		if n > 0 {
			s.chunks <- b[:n]
		}
		if err != nil {
			return
		}
	}
}

func (s *Session) text() string {
	return strings.ReplaceAll(string(s.buf), "\r", "")
}

func (s *Session) learnPrompt() error {
	timer := time.NewTimer(loginTimeout)
	defer timer.Stop()
	for {
		if m := promptRe.FindStringSubmatch(s.text()); m != nil {
			if strings.Contains(strings.ToUpper(m[1]), "Y/N") {
				return fmt.Errorf("router asked an interactive question after login: %q", strings.TrimSpace(s.text()))
			}
			s.prompt = m[1]
			s.buf = s.buf[:0]
			return nil
		}
		select {
		case chunk, ok := <-s.chunks:
			if !ok {
				return fmt.Errorf("%w: during login", ErrClosed)
			}
			s.buf = append(s.buf, chunk...)
		case <-timer.C:
			return fmt.Errorf("%w: no prompt after login", ErrTimeout)
		}
	}
}

// Run sends cmd and returns its output without the command echo and the prompt.
func (s *Session) Run(cmd string, timeout time.Duration) (string, error) {
	s.drain()
	if _, err := io.WriteString(s.stdin, cmd+"\n"); err != nil {
		s.Close()
		return "", fmt.Errorf("%w: write: %v", ErrClosed, err)
	}
	out, err := s.waitPrompt(timeout)
	switch {
	case err == nil:
		return cleanOutput(out, cmd), nil
	case errors.Is(err, ErrTimeout):
		_, _ = io.WriteString(s.stdin, "\x03")
		if _, err := s.waitPrompt(ctrlCTimeout); err != nil {
			s.Close()
			return "", fmt.Errorf("%w: %w: no prompt after Ctrl-C", ErrTimeout, ErrClosed)
		}
		return "", ErrTimeout
	default:
		s.Close()
		return "", err
	}
}

// drain drops output left over from an interrupted command.
func (s *Session) drain() {
	s.buf = s.buf[:0]
	for {
		select {
		case _, ok := <-s.chunks:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (s *Session) waitPrompt(timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		trimmed := strings.TrimRight(s.text(), " \n")
		if strings.HasSuffix(trimmed, s.prompt) {
			s.buf = s.buf[:0]
			return strings.TrimSuffix(trimmed, s.prompt), nil
		}
		select {
		case chunk, ok := <-s.chunks:
			if !ok {
				return "", ErrClosed
			}
			s.buf = append(s.buf, chunk...)
		case <-timer.C:
			return "", ErrTimeout
		}
	}
}

func cleanOutput(out, cmd string) string {
	lines := strings.Split(out, "\n")
	if len(lines) > 0 && strings.Contains(lines[0], cmd) {
		lines = lines[1:]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// Close ends the shell and the SSH connection.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		_ = s.sess.Close()
		err = s.client.Close()
	})
	return err
}
