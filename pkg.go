// Package dietdog couples [log.Logger] from the standard library with the DataDog logs API.
package dietdog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Options customize behavior of this package.
type Option func(*config)

// WithAuth configures DataDog API key to use.
func WithAuth(token string) Option { return func(c *config) { c.auth = token } }

// New returns an io.WriteCloser which can be used as a log.Logger underlying writer.
// It buffers logged messages and submits them to the DataDog logs API endpoint.
//
// To get it working, you need to create a new writer providing an API key:
//
//	sink := dietdog.New(dietdog.WithAuth(apiKey))
//	defer sink.Close()
//	logger := log.New(io.MultiWriter(os.Stderr, sink), "", 0)
//	logger.Println("Hello, world!")
//
// When used without an API key, writes to it become no-op.
//
// On a high log rate writes may occasionally block until the background log consumer catches up.
//
// To release resouces and trigger the unsubmitted logs delivery, call its Close method.
// Be careful not to call Fatal* and Panic* methods of its parent Logger.
// Because this package delivers logs to DataDog asynchronously,
// it won't have time to deliver those final messages,
// because Fatal* and Panic* methods terminate the program.
func New(opts ...Option) *writer {
	s := &writer{}
	for _, opt := range opts {
		opt(&s.config)
	}
	if s.auth == "" && s.log != nil {
		s.log.Print("dietdog.New called without an auth option, its writes would be no-op")
	}
	return s
}

// WithEndpoint overrides which endpoint logs are sent to.
func WithEndpoint(url string) Option { return func(c *config) { c.url = url } }

// WithMetadata adds additional metatada to every log message sent.
func WithMetadata(m Metadata) Option {
	return func(c *config) {
		c.tags = m.Tags
		c.source = m.Source
		c.service = m.Service
		c.hostname = m.Hostname
	}
}

// Metadata describes additional fields of a log message.
// See [DataDog API documentation] for more details.
//
// [DataDog API documentation]: https://docs.datadoghq.com/api/latest/logs/#send-logs
type Metadata struct {
	Tags     string // ddtags
	Source   string // ddsource
	Service  string // service
	Hostname string // hostname
}

// WithLogger configures writer with an additional logger to log its own errors to.
// Some errors it may log include: failed delivery attempts, dropped messages because of internal queue overflow.
//
// Be careful not to provide Logger that you plan to reconfigure to write to this writer.
// Such configuration may eventually result in a deadlock.
//
// It is recommended to use a dedicated logger with a custom prefix:
//
//	sink := dietdog.New(dietdog.WithAuth(apiKey),
//	  dietdog.WithLogger(log.New(os.Stderr, "DataDog logging: ", 0)))
func WithLogger(l *log.Logger) Option { return func(c *config) { c.log = l } }

type config struct {
	tags     string
	source   string
	service  string
	hostname string

	auth string
	url  string
	log  *log.Logger
}

type writer struct {
	config

	once     sync.Once
	ch       chan rawMessage
	loopDone chan struct{} // closed on loop exit
	ctx      context.Context
	cancel   context.CancelFunc
	gzw      *gzip.Writer // reused by sendBatch calls
}

// Write puts a message coming from a *log.Logger into an internal queue.
// On a high log rate it may occasionally block until the background log consumer catches up.
func (s *writer) Write(p []byte) (int, error) {
	if s.auth == "" {
		return len(p), nil
	}
	s.once.Do(s.init)
	if len(p) == 0 {
		return 0, nil
	}
	m := rawMessage{t: time.Now().UnixMilli(), b: make([]byte, len(p))}
	copy(m.b, p) // io.Write implementations must not retain p
	select {
	case <-s.ctx.Done():
		return 0, errors.New("sink is already closed")
	case s.ch <- m:
	}
	return len(p), nil
}

// Close triggers unsent logs final delivery and release resources.
// It blocks until the final delivery attempt finishes.
func (s *writer) Close() error {
	if s.auth == "" {
		return nil
	}
	s.once.Do(s.init)
	s.cancel()
	<-s.loopDone
	return nil
}

func (s *writer) init() {
	if s.url == "" {
		s.url = DefaultIntakeEndpoint
	}
	s.ch = make(chan rawMessage, 1000)
	s.loopDone = make(chan struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.gzw, _ = gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	if s.log == nil {
		s.log = log.New(io.Discard, "", 0)
	}
	go s.loop()
}

func (s *writer) loop() {
	defer close(s.loopDone)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var batch []rawMessage
	resetBatch := func() {
		for i := range batch {
			batch[i].b = nil
		}
		batch = batch[:0]
	}
	for {
		select {
		case m := <-s.ch:
			if len(batch) == maxBatchSize {
				s.log.Print("outgoing queue overflow, dropping message")
				continue
			}
			batch = append(batch, m)
			if len(batch) < maxBatchSize {
				continue
			}
		case <-ticker.C:
		case <-s.ctx.Done():
		drain:
			for l := len(batch); l < maxBatchSize; {
				select {
				case m := <-s.ch:
					batch = append(batch, m)
				default:
					break drain
				}
			}
			if err := s.sendBatch(batch); err != nil {
				s.log.Printf("final batch send: %v, dropped %d messages", err, len(batch))
			}
			return
		}
		if len(batch) == 0 {
			continue
		}
		if err := s.sendBatch(batch); err != nil {
			var r *errBadStatus
			if errors.As(err, &r) && r.retryable() {
				continue
			}
			var t interface{ Timeout() bool }
			if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &t) && t.Timeout()) {
				continue
			}
			s.log.Printf("batch send: %v, dropping %d messages", err, len(batch))
		}
		resetBatch()
	}
}

type rawMessage struct {
	t int64 // unix timestamp in milliseconds
	b []byte
}

const maxBatchSize = 1000 // https://docs.datadoghq.com/api/latest/logs/#send-logs

func (s *writer) sendBatch(batch []rawMessage) error {
	if len(batch) == 0 {
		return nil
	}
	if len(batch) > maxBatchSize {
		batch = batch[:maxBatchSize]
	}
	type msg struct {
		Source   string `json:"ddsource,omitempty"`
		Tags     string `json:"ddtags,omitempty"`
		Hostname string `json:"hostname,omitempty"`
		Service  string `json:"service,omitempty"`
		Message  string `json:"message"`
		TS       int64  `json:"timestamp"`
	}
	out := make([]msg, len(batch))
	for i := range batch {
		out[i] = msg{
			Source:   s.source,
			Tags:     s.tags,
			Hostname: s.hostname,
			Service:  s.service,
			Message:  string(batch[i].b),
			TS:       batch[i].t,
		}
	}
	buf := new(bytes.Buffer) // TODO: maybe pool
	s.gzw.Reset(buf)
	if err := json.NewEncoder(s.gzw).Encode(out); err != nil {
		return err
	}
	if err := s.gzw.Close(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "github.com/artyom/dietdog")
	req.Header.Set("DD-API-KEY", s.auth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &errBadStatus{code: resp.StatusCode}
}

type errBadStatus struct{ code int }

func (e *errBadStatus) Error() string { return fmt.Sprintf("unexpected status code: %d", e.code) }
func (e *errBadStatus) retryable() bool {
	return e.code == http.StatusTooManyRequests || e.code >= 500
}

// DefaultIntakeEndpoint is a default endpoint to send logs
const DefaultIntakeEndpoint = "https://http-intake.logs.datadoghq.com/api/v2/logs"
