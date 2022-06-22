package dietdog

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestWriterNoAuth(t *testing.T) {
	sink := New()
	sink.Write([]byte("test"))
	defer sink.Close()
	if sink.ch != nil {
		t.Fatal("writer without auth token should be a no-op, but it's initialized")
	}
}

func TestWriterRoundtrip(t *testing.T) {
	lines := []string{"one", "two", "three"}
	begin := time.Now()
	var seenLines []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("Content-Encoding") == "gzip" {
			if buf, err = ungzip(buf); err != nil {
				t.Errorf("decompressing body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		msgs := []struct {
			Message string `json:"message"`
			TS      int64  `json:"timestamp"`
		}{}
		if err := json.Unmarshal(buf, &msgs); err != nil {
			t.Errorf("unmarshaling body payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, m := range msgs {
			seenLines = append(seenLines, m.Message)
			ts := time.UnixMilli(m.TS)
			if diff := ts.Sub(begin); diff < -time.Second || diff > time.Second {
				t.Errorf("timestamp %d (%v) is way too off (%v compared to start time %v)",
					m.TS, ts, diff, begin)
			}
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sink := New(WithAuth("x"), WithEndpoint(srv.URL), WithLogger(log.New(os.Stderr, "SINK: ", 0)))
	for _, line := range lines {
		if _, err := sink.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	sink.Close()

	if len(lines) != len(seenLines) {
		t.Fatalf("want: %q\ngot: %q", lines, seenLines)
	}
	for i := range lines {
		if lines[i] != seenLines[i] {
			t.Fatalf("want: %q\ngot: %q", lines, seenLines)
		}
	}
}

func ungzip(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func BenchmarkWriter(b *testing.B) {
	b.StopTimer()
	if s := os.Getenv("LISTENER_FD"); os.Getenv("START_LISTENER") == "true" && s != "" {
		fd, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}
		log.Fatal(serve(fd))
	}
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	tln, ok := ln.(*net.TCPListener)
	if !ok {
		b.Fatalf("listener %+v is not a *net.TCPListener", ln)
	}
	f, err := tln.File()
	if err != nil {
		b.Fatalf("getting *os.File from *net.TCPListener: %v", err)
	}
	defer f.Close()
	cmd := exec.Command(os.Args[0], "-test.run==^$", "-test.bench=^BenchmarkWriter$")
	cmd.Env = append(cmd.Environ(), "START_LISTENER=true", "LISTENER_FD="+strconv.Itoa(3+len(cmd.ExtraFiles)))
	cmd.ExtraFiles = append(cmd.ExtraFiles, f)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		b.Fatalf("starting subprocess: %v", err)
	}
	defer func() {
		cmd.Process.Signal(os.Interrupt)
		time.Sleep(10 * time.Millisecond)
		cmd.Process.Kill()
		cmd.Wait()
	}()
	b.Run("Write", func(b *testing.B) {
		sink := New(WithAuth("x"), WithEndpoint("http://"+ln.Addr().String()+"/"), WithLogger(log.New(os.Stdout, "SINK: ", 0)))
		defer sink.Close()
		const line = "The following flags are recognized by the 'go test' command and control the execution of any test:"
		b.SetBytes(int64(len(line)))
		b.ResetTimer()
		b.ReportAllocs()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			sink.Write([]byte(line))
		}
	})
}

func serve(fd int) error {
	ln, err := net.FileListener(os.NewFile(uintptr(fd), "ln"))
	if err != nil {
		return err
	}
	defer ln.Close()
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusAccepted)
		}),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return srv.Serve(ln)
}
