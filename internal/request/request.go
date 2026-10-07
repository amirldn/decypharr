package request

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"go.uber.org/ratelimit"
	"golang.org/x/net/proxy"
)

var (
	once     sync.Once
	instance *Client
)

type ClientOption func(*Client)

// Client represents an HTTP client with additional capabilities
type Client struct {
	client          *retryablehttp.Client
	httpClient      *http.Client // underlying http client
	rateLimiter     ratelimit.Limiter
	headers         map[string]string
	headersMu       sync.RWMutex
	maxRetries      int
	throttle        *Throttle
	retryWaitMin    time.Duration
	retryWaitMax    time.Duration
	timeout         time.Duration
	skipTLSVerify   bool
	retryableStatus map[int]struct{}
	logger          zerolog.Logger
	proxy           string
}

// WithRetryWait sets the retry wait range
func WithRetryWait(min, max time.Duration) ClientOption {
	return func(c *Client) {
		c.retryWaitMin = min
		c.retryWaitMax = max
	}
}

// WithMaxRetries sets the maximum number of retry attempts
func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

// WithTimeout sets the request timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithRateLimiter sets a rate limiter
func WithRateLimiter(rl ratelimit.Limiter) ClientOption {
	return func(c *Client) {
		c.rateLimiter = rl
	}
}

// WithHeaders sets default headers
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *Client) {
		c.headersMu.Lock()
		c.headers = headers
		c.headersMu.Unlock()
	}
}

func (c *Client) SetHeader(key, value string) {
	c.headersMu.Lock()
	c.headers[key] = value
	c.headersMu.Unlock()
}

func WithLogger(logger zerolog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

func WithTransport(transport *http.Transport) ClientOption {
	return func(c *Client) {
		c.httpClient.Transport = transport
	}
}

// WithRetryableStatus adds status codes that should trigger a retry
func WithRetryableStatus(statusCodes ...int) ClientOption {
	return func(c *Client) {
		c.retryableStatus = make(map[int]struct{}) // reset the map
		for _, code := range statusCodes {
			c.retryableStatus[code] = struct{}{}
		}
	}
}

func WithProxy(proxyURL string) ClientOption {
	return func(c *Client) {
		c.proxy = proxyURL
	}
}

// Do performs an HTTP request with retries for certain status codes
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	// Apply headers
	c.headersMu.RLock()
	if c.headers != nil {
		for key, value := range c.headers {
			req.Header.Set(key, value)
		}
	}
	c.headersMu.RUnlock()

	// Apply rate limiting
	if c.rateLimiter != nil && c.throttle == nil {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		default:
			c.rateLimiter.Take()
		}
	}

	// Convert to retryablehttp request
	retryReq, err := retryablehttp.FromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("creating retryable request: %w", err)
	}

	return c.client.Do(retryReq)
}

// DoOnce uses the configured transport, headers, limiter, and throttle without
// the retry client's status retry loop. It is used for scarce requestdl calls:
// a retry must not spend another budget admission for the same link attempt.
func (c *Client) DoOnce(req *http.Request) (*http.Response, error) {
	c.headersMu.RLock()
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	c.headersMu.RUnlock()
	if c.rateLimiter != nil && c.throttle == nil {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		c.rateLimiter.Take()
	}
	return c.httpClient.Do(req)
}

// MakeRequest performs an HTTP request and returns the response body as bytes
func (c *Client) MakeRequest(req *http.Request) ([]byte, error) {
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			c.logger.Printf("Failed to close response body: %v", err)
		}
	}()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error %d: %s", res.StatusCode, string(bodyBytes))
	}

	return bodyBytes, nil
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}

	return c.Do(req)
}

// zerologAdapter bridges zerolog to the retryablehttp.Logger interface so that
// retry events (including 429 backoffs) appear in decypharr's structured log.
type zerologAdapter struct{ log zerolog.Logger }

func (z zerologAdapter) Printf(format string, args ...interface{}) {
	z.log.Debug().Msgf(format, args...)
}

// retryAfterBackoff uses the WithRetryWait ceiling, including Retry-After.
// TorBox raises that ceiling to minutes; other clients retain their defaults.
func retryAfterBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		return retryablehttp.DefaultBackoff(min, max, attemptNum, resp)
	}
	ra := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if secs, err := strconv.ParseUint(ra, 10, 64); err == nil && secs > 0 {
		if secs > uint64(max/time.Second) {
			return max
		}
		return minDuration(max, time.Duration(secs)*time.Second)
	}
	if at, err := http.ParseTime(ra); err == nil {
		if wait := time.Until(at); wait > 0 {
			return minDuration(max, wait)
		}
	}
	// Equal jitter retains exponential growth and never exceeds the ceiling.
	ceiling := min
	for i := 0; i < attemptNum && ceiling < max; i++ {
		if ceiling > max/2 {
			ceiling = max
			break
		}
		ceiling *= 2
	}
	if ceiling > max {
		ceiling = max
	}
	if ceiling <= 0 {
		return 0
	}
	half := ceiling / 2
	return half + time.Duration(rand.Int64N(int64(ceiling-half)+1))
}
func minDuration(ceiling, wait time.Duration) time.Duration {
	if wait < 0 {
		return 0
	}
	if wait > ceiling {
		return ceiling
	}
	return wait
}

// New creates a new HTTP client with the specified options
func New(options ...ClientOption) *Client {
	client := &Client{
		maxRetries:    5,
		skipTLSVerify: true,
		retryableStatus: map[int]struct{}{
			http.StatusTooManyRequests:     {},
			http.StatusInternalServerError: {},
			http.StatusBadGateway:          {},
			http.StatusServiceUnavailable:  {},
			http.StatusGatewayTimeout:      {},
		},
		logger:  logger.New("request"),
		timeout: 60 * time.Second,
		proxy:   "",
		headers: make(map[string]string),
	}

	// Create default http client
	client.httpClient = &http.Client{
		Timeout: client.timeout,
	}

	// Apply options before configuring transport
	for _, option := range options {
		option(client)
	}

	client.httpClient.Timeout = client.timeout

	// Check if transport was set by WithTransport option
	if client.httpClient.Transport == nil {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: client.skipTLSVerify,
			},
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		}

		// Configure proxy if needed
		SetProxy(transport, client.proxy)

		// Set the transport to the client
		client.httpClient.Transport = transport
	}

	if client.throttle != nil {
		client.httpClient.Transport = &throttleTransport{next: client.httpClient.Transport, throttle: client.throttle, limiter: client.rateLimiter}
	}

	// Create retryablehttp client
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient = client.httpClient
	retryClient.RetryMax = client.maxRetries
	if client.retryWaitMin > 0 {
		retryClient.RetryWaitMin = client.retryWaitMin
	} else {
		retryClient.RetryWaitMin = 1 * time.Second
	}
	if client.retryWaitMax > 0 {
		retryClient.RetryWaitMax = client.retryWaitMax
	} else {
		retryClient.RetryWaitMax = 30 * time.Second
	}
	retryClient.Logger = nil
	retryClient.Backoff = retryAfterBackoff
	if client.throttle != nil {
		retryClient.Backoff = func(min, max time.Duration, attempt int, resp *http.Response) time.Duration {
			if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
				return client.throttle.Remaining()
			}
			return retryAfterBackoff(min, max, attempt, resp)
		}
	}

	// Preserve the final HTTP response on exhausted retries instead of discarding it.
	retryClient.ErrorHandler = func(resp *http.Response, err error, numTries int) (*http.Response, error) {
		if resp != nil {
			if resp.Header != nil && numTries > 0 {
				resp.Header.Set("X-Decypharr-Attempts", strconv.Itoa(numTries))
			}
			return resp, err
		}
		if err == nil {
			return nil, fmt.Errorf("giving up after %d attempt(s)", numTries)
		}
		return nil, fmt.Errorf("giving up after %d attempt(s): %w", numTries, err)
	}

	// Custom retry policy based on retryable status codes
	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// Don't retry on context errors
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if e := BackpressureError(err); e != nil {
			return false, e
		}
		// A provider 429 ends this operation. The shared transport gate holds
		// later operations until the full server deadline; retryablehttp must
		// never schedule another attempt from this response.
		if client.throttle != nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			return false, nil
		}
		if client.throttle != nil && client.throttle.isOpen() {
			return false, err
		}

		// Use the default policy for transport errors. HTTP responses use the
		// configured status list so provider errors retain their response body.
		if err != nil || resp == nil {
			return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		}

		// Check for retryable status codes (only if resp is not nil)
		if resp != nil {
			if _, ok := client.retryableStatus[resp.StatusCode]; ok {
				return true, nil
			}
		}

		return false, nil
	}

	client.client = retryClient

	return client
}

func Default() *Client {
	once.Do(func() {
		instance = New()
	})
	return instance
}

func SetProxy(transport *http.Transport, proxyURL string) {
	if proxyURL != "" {
		if strings.HasPrefix(proxyURL, "socks5://") {
			// Handle SOCKS5 proxy
			socksURL, err := url.Parse(proxyURL)
			if err == nil {
				auth := &proxy.Auth{}
				if socksURL.User != nil {
					auth.User = socksURL.User.Username()
					password, _ := socksURL.User.Password()
					auth.Password = password
				}

				dialer, err := proxy.SOCKS5("tcp", socksURL.Host, auth, proxy.Direct)
				if err == nil {
					transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
						return dialer.Dial(network, addr)
					}
				}
			}
		} else {
			_proxy, err := url.Parse(proxyURL)
			if err == nil {
				transport.Proxy = http.ProxyURL(_proxy)
			}
		}
	} else {
		transport.Proxy = http.ProxyFromEnvironment
	}
}
