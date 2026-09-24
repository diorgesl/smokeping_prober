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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const fakePrompt = "<rt-test>"

// fakeVRP is an in-process SSH server that behaves like a Huawei VRP user view.
type fakeVRP struct {
	password    string
	banner      string            // printed before the first prompt
	loginText   string            // when set, replaces banner and prompt entirely
	responses   map[string]string // command -> output
	hang        map[string]bool   // commands that only end with Ctrl-C
	ignoreCtrlC bool
	// extraPromptOnCtrlC makes the Ctrl-C reply to a hanging command send the
	// prompt twice, the second one 50ms later, to simulate a stray prompt
	// racing the next Run call.
	extraPromptOnCtrlC bool
	// stallShell makes the server never reply to a "shell" channel request,
	// simulating a router that accepted the connection but never answers.
	stallShell bool

	addr    string
	hostKey ssh.PublicKey

	mu   sync.Mutex
	cmds []string
}

func startFakeVRP(t *testing.T, f *fakeVRP) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	f.hostKey = signer.PublicKey()
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if string(p) == f.password {
				return nil, nil
			}
			return nil, errors.New("access denied")
		},
	}
	cfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f.addr = ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn, cfg)
		}
	}()
}

func (f *fakeVRP) knownHosts(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := knownhosts.Line([]string{knownhosts.Normalize(f.addr)}, f.hostKey)
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (f *fakeVRP) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cmds...)
}

func (f *fakeVRP) serve(conn net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		conn.Close()
		return
	}
	go ssh.DiscardRequests(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			nc.Reject(ssh.UnknownChannelType, "only sessions")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			return
		}
		go func() {
			for r := range creqs {
				switch r.Type {
				case "pty-req":
					r.Reply(true, nil)
				case "shell":
					if f.stallShell {
						// Never reply: the client's Shell() call blocks
						// forever unless the connection is closed.
						continue
					}
					r.Reply(true, nil)
					go f.shell(ch)
				default:
					r.Reply(false, nil)
				}
			}
		}()
	}
}

func (f *fakeVRP) shell(ch ssh.Channel) {
	defer ch.Close()
	if f.loginText != "" {
		io.WriteString(ch, f.loginText)
	} else {
		io.WriteString(ch, f.banner+"\r\n"+fakePrompt)
	}
	var line []byte
	hanging := false
	b := make([]byte, 1)
	for {
		if _, err := ch.Read(b); err != nil {
			return
		}
		switch c := b[0]; {
		case c == 0x03:
			if hanging && !f.ignoreCtrlC {
				hanging = false
				io.WriteString(ch, "\r\n"+fakePrompt)
				if f.extraPromptOnCtrlC {
					go func() {
						time.Sleep(50 * time.Millisecond)
						io.WriteString(ch, "\r\n"+fakePrompt)
					}()
				}
			}
		case c == '\r' || c == '\n':
			if hanging {
				continue
			}
			cmd := strings.TrimSpace(string(line))
			line = line[:0]
			if cmd == "" {
				continue
			}
			f.mu.Lock()
			f.cmds = append(f.cmds, cmd)
			f.mu.Unlock()
			io.WriteString(ch, cmd+"\r\n") // terminal echo
			if f.hang[cmd] {
				hanging = true
				continue
			}
			out, ok := f.responses[cmd]
			if !ok && cmd == "screen-length 0 temporary" {
				out = "Info: The configuration takes effect on the current user terminal interface only."
			}
			if out != "" {
				io.WriteString(ch, strings.ReplaceAll(out, "\n", "\r\n")+"\r\n")
			}
			io.WriteString(ch, "\r\n"+fakePrompt)
		default:
			if !hanging {
				line = append(line, c)
			}
		}
	}
}

func mustEd25519(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}
