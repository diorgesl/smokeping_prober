// Copyright The Prometheus Authors
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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var testJob = Job{Target: "1.1.1.1", Source: "201.131.152.1", Count: 3, PacketInterval: ms(50), Timeout: ms(500), Size: 56}

func dialFake(t *testing.T, f *fakeVRP) (*Session, error) {
	t.Helper()
	return Dial(context.Background(), SSHConfig{
		Address:        f.addr,
		Username:       "smokeping",
		Password:       "secret",
		KnownHostsFile: f.knownHosts(t),
		DialTimeout:    2 * time.Second,
	})
}

func TestDialAndRunPing(t *testing.T) {
	cmd := BuildCommand(testJob)
	f := &fakeVRP{password: "secret", responses: map[string]string{cmd: ipv4OK}}
	startFakeVRP(t, f)

	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	out, err := s.Run(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(out, fakePrompt) {
		t.Errorf("output still contains the prompt: %q", out)
	}
	if strings.HasPrefix(out, "ping ") {
		t.Errorf("output still starts with the command echo: %q", out)
	}
	got, err := ParseOutput(out)
	if err != nil {
		t.Fatalf("ParseOutput: %v", err)
	}
	want, _ := ParseOutput(ipv4OK)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if cmds := f.commands(); len(cmds) != 2 || cmds[0] != "screen-length 0 temporary" || cmds[1] != cmd {
		t.Errorf("commands = %q", cmds)
	}
}

func TestDialSkipsBanner(t *testing.T) {
	f := &fakeVRP{
		password: "secret",
		banner:   "Info: The max number of VTY users is 21, the number of current VTY users online is 2.\r\n      The current login time is 2026-09-24 10:00:00.",
	}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	s.Close()
}

func TestDialRejectsInteractiveQuestion(t *testing.T) {
	f := &fakeVRP{
		password:  "secret",
		loginText: "Warning: The password has expired. Change now? [Y/N]",
	}
	startFakeVRP(t, f)
	_, err := dialFake(t, f)
	if err == nil || !strings.Contains(err.Error(), "interactive question") {
		t.Fatalf("error = %v, want interactive question error", err)
	}
}

func TestDialRejectsInteractiveQuestionWithColon(t *testing.T) {
	f := &fakeVRP{
		password:  "secret",
		loginText: "Warning: The password has expired. Change now? [Y/N]:",
	}
	startFakeVRP(t, f)
	start := time.Now()
	_, err := dialFake(t, f)
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Dial took %v, want well under loginTimeout", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "interactive question") {
		t.Fatalf("error = %v, want interactive question error", err)
	}
}

func TestDialRejectsUnknownHostKey(t *testing.T) {
	f := &fakeVRP{password: "secret"}
	startFakeVRP(t, f)
	other, err := ssh.NewSignerFromKey(mustEd25519(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := knownhosts.Line([]string{knownhosts.Normalize(f.addr)}, other.PublicKey())
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Dial(context.Background(), SSHConfig{Address: f.addr, Username: "u", Password: "secret", KnownHostsFile: path, DialTimeout: 2 * time.Second})
	var keyErr *knownhosts.KeyError
	if !errors.As(err, &keyErr) {
		t.Fatalf("error = %v, want a *knownhosts.KeyError", err)
	}
}

func TestDialWrongPassword(t *testing.T) {
	f := &fakeVRP{password: "secret"}
	startFakeVRP(t, f)
	_, err := Dial(context.Background(), SSHConfig{Address: f.addr, Username: "u", Password: "wrong", KnownHostsFile: f.knownHosts(t), DialTimeout: 2 * time.Second})
	if err == nil {
		t.Fatal("expected authentication error")
	}
}

func TestRunTimeoutRecoversWithCtrlC(t *testing.T) {
	cmd := BuildCommand(testJob)
	f := &fakeVRP{password: "secret", hang: map[string]bool{"ping hang": true}, responses: map[string]string{cmd: ipv4OK}}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	if _, err := s.Run("ping hang", 200*time.Millisecond); !errors.Is(err, ErrTimeout) || errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrTimeout only", err)
	}
	out, err := s.Run(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("session unusable after Ctrl-C: %v", err)
	}
	if _, err := ParseOutput(out); err != nil {
		t.Errorf("ParseOutput after recovery: %v", err)
	}
}

func TestRunTimeoutWithoutPromptClosesSession(t *testing.T) {
	f := &fakeVRP{password: "secret", hang: map[string]bool{"ping hang": true}, ignoreCtrlC: true}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_, err = s.Run("ping hang", 200*time.Millisecond)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrTimeout and ErrClosed", err)
	}
}

// TestRunIgnoresStrayPrompt covers a race where the Ctrl-C reply to a hanging
// command is followed by a second, stray prompt that lands after the next
// Run has already started: without anchoring on the command's own echo, that
// stray prompt would be mistaken for the end of the following command and
// its real output would bleed into the command after that.
func TestRunIgnoresStrayPrompt(t *testing.T) {
	cmdA := BuildCommand(testJob)
	jobB := testJob
	jobB.IPv6 = true
	cmdB := BuildCommand(jobB)

	f := &fakeVRP{
		password:           "secret",
		hang:               map[string]bool{"ping hang": true},
		extraPromptOnCtrlC: true,
		responses:          map[string]string{cmdA: ipv4OK, cmdB: ipv6OK},
	}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	if _, err := s.Run("ping hang", 200*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", err)
	}

	outA, err := s.Run(cmdA, 2*time.Second)
	if err != nil {
		t.Fatalf("Run(cmdA): %v", err)
	}
	gotA, err := ParseOutput(outA)
	if err != nil {
		t.Fatalf("ParseOutput(cmdA output): %v", err)
	}
	if wantA, _ := ParseOutput(ipv4OK); !reflect.DeepEqual(gotA, wantA) {
		t.Errorf("cmdA: got %+v, want %+v", gotA, wantA)
	}

	outB, err := s.Run(cmdB, 2*time.Second)
	if err != nil {
		t.Fatalf("Run(cmdB): %v", err)
	}
	gotB, err := ParseOutput(outB)
	if err != nil {
		t.Fatalf("ParseOutput(cmdB output): %v", err)
	}
	if wantB, _ := ParseOutput(ipv6OK); !reflect.DeepEqual(gotB, wantB) {
		t.Errorf("cmdB: got %+v, want %+v", gotB, wantB)
	}
}

// TestRunWithWrappedEcho covers a router that wraps and redraws a long
// command line instead of echoing it on one line: anchoring on the whole
// command (as opposed to just its last token, the destination) would never
// match a wrapped echo split across "\r\n". wrapEchoAt is chosen so the
// command's final token (the IPv6 destination) is not itself split across a
// wrap boundary; a split destination token would still work as long as the
// same text later appears intact in the "PING <dst>" response header, which
// Run's anchor search would then match instead, but that fallback is not
// what this test exercises.
func TestRunWithWrappedEcho(t *testing.T) {
	job := testJob
	job.Target = "2001:4860:4860::8888"
	job.Source = "2804:194c:1000::155:f0ca:a"
	job.IPv6 = true
	cmd := BuildCommand(job)

	const wrapAt = 50
	start := strings.LastIndex(cmd, job.Target)
	if start == -1 {
		t.Fatalf("destination %q not found in command %q", job.Target, cmd)
	}
	if start/wrapAt != (start+len(job.Target)-1)/wrapAt {
		t.Fatalf("wrapAt=%d splits the destination token in %q; pick a different wrap width", wrapAt, cmd)
	}

	f := &fakeVRP{password: "secret", wrapEchoAt: wrapAt, responses: map[string]string{cmd: ipv6OK}}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	out, err := s.Run(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := ParseOutput(out)
	if err != nil {
		t.Fatalf("ParseOutput: %v", err)
	}
	want, _ := ParseOutput(ipv6OK)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestDialHonorsContext covers a router that accepts the SSH handshake but
// never answers the shell channel request: without closing the underlying
// connection on ctx cancellation, Dial would block forever.
func TestDialHonorsContext(t *testing.T) {
	f := &fakeVRP{password: "secret", stallShell: true}
	startFakeVRP(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := Dial(ctx, SSHConfig{
		Address:        f.addr,
		Username:       "smokeping",
		Password:       "secret",
		KnownHostsFile: f.knownHosts(t),
		DialTimeout:    2 * time.Second,
	})
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Dial took %v, want to return once ctx is done", elapsed)
	}
	if err == nil {
		t.Fatal("expected an error when ctx is cancelled during login")
	}
}
