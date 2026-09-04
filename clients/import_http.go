package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/config"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const (
	MaxTemporaryProbeFileSizeBytes int64 = config.MaxInputFileSizeBytes
	MaxTemporaryHLSProbeSizeBytes  int64 = 1 << 30
	MaxTemporaryHLSKeySizeBytes    int64 = 1 << 20
)

type publicNetworkTransport struct {
	base      http.RoundTripper
	surface   string
	allowHTTP bool
}

func (t *publicNetworkTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	allowedSchemes := []string{"https"}
	if t.allowHTTP {
		allowedSchemes = append(allowedSchemes, "http")
	}
	if err := ValidatePublicURL(req.URL, t.surface, true, allowedSchemes...); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(req)
}

// DestinationPolicyError identifies a request-controlled destination that was
// rejected before a connection was opened. Callers may use this distinction to
// return a 4xx response without exposing the destination or embedded secrets.
type DestinationPolicyError struct {
	Surface string
	Reason  string
}

func (e *DestinationPolicyError) Error() string {
	return fmt.Sprintf("%s destination rejected: %s", e.Surface, e.Reason)
}

func IsDestinationPolicyError(err error) bool {
	var policyErr *DestinationPolicyError
	return errors.As(err, &policyErr)
}

func rejectDestination(surface, format string, args ...any) error {
	return &DestinationPolicyError{Surface: surface, Reason: fmt.Sprintf(format, args...)}
}

var nonPublicPrefixes = []netip.Prefix{
	// Shared, documentation, benchmarking, and otherwise non-public IPv4 ranges
	// which netip.Addr.IsGlobalUnicast reports as global unicast.
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),

	// IPv6 transition and special-use ranges which can route to, or encode,
	// non-public destinations.
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

func normalizeHostname(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func isLocalHostname(host string) bool {
	host = normalizeHostname(host)
	return host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local")
}

func isPublicIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if addr.Zone() != "" {
		addr = addr.WithZone("")
	}
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// ValidatePublicURL rejects URLs that obviously target local or non-public
// resources. Hostnames are resolved and revalidated by the public-network
// transport immediately before a connection is opened.
func ValidatePublicURL(u *url.URL, surface string, allowUserInfo bool, allowedSchemes ...string) error {
	if u == nil {
		return rejectDestination(surface, "URL is nil")
	}

	scheme := strings.ToLower(u.Scheme)
	allowed := false
	for _, candidate := range allowedSchemes {
		if scheme == candidate {
			allowed = true
			break
		}
	}
	if !allowed {
		return rejectDestination(surface, "URL scheme %q is not allowed", u.Scheme)
	}
	if !allowUserInfo && u.User != nil {
		return rejectDestination(surface, "URL userinfo is not allowed")
	}

	host := u.Hostname()
	if host == "" {
		return rejectDestination(surface, "URL has no host")
	}
	if isLocalHostname(host) {
		return rejectDestination(surface, "URL host %q is not public", host)
	}
	if addr, err := netip.ParseAddr(host); err == nil && !isPublicIP(net.IP(addr.AsSlice())) {
		return rejectDestination(surface, "URL host %q is not public", host)
	}
	return nil
}

// ValidateImportURL applies the schemes supported by VOD source imports.
func ValidateImportURL(u *url.URL) error {
	return ValidatePublicURL(u, "source", true, "http", "https", "s3", "s3+http", "s3+https", SCHEME_IPFS, SCHEME_ARWEAVE)
}

func publicNetworkControl(surface string) func(context.Context, string, string, syscall.RawConn) error {
	return func(_ context.Context, _, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return rejectDestination(surface, "invalid address: %v", err)
		}
		addr, err := netip.ParseAddr(host)
		if err != nil || !isPublicIP(net.IP(addr.AsSlice())) {
			return rejectDestination(surface, "refusing to connect to non-public address %q", host)
		}
		return nil
	}
}

func newImportHTTPClient(timeout time.Duration) *http.Client {
	return newPublicHTTPClientForSurface(timeout, "source", true, true)
}

func newPublicHTTPClientForSurface(timeout time.Duration, surface string, allowHTTP, followRedirects bool) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// An HTTP proxy would resolve and dial the destination outside this
	// transport, bypassing the address checks below.
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{
		Timeout:        30 * time.Second,
		KeepAlive:      30 * time.Second,
		ControlContext: publicNetworkControl(surface),
	}).DialContext
	return newPublicHTTPClientWithTransportForSurface(timeout, surface, allowHTTP, followRedirects, transport)
}

func newPublicHTTPClientWithTransportForSurface(timeout time.Duration, surface string, allowHTTP, followRedirects bool, transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: &publicNetworkTransport{base: transport, surface: surface, allowHTTP: allowHTTP},
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !followRedirects {
				return rejectDestination(surface, "redirects are not allowed")
			}
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			allowedSchemes := []string{"https"}
			if allowHTTP {
				allowedSchemes = append(allowedSchemes, "http")
			}
			if err := ValidatePublicURL(req.URL, surface, true, allowedSchemes...); err != nil {
				return err
			}
			return nil
		},
	}
}

type PublicImportURL struct{ url *url.URL }

func ParsePublicImportURL(rawURL string) (PublicImportURL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return PublicImportURL{}, catErrs.Unretriable(catErrs.Public(catErrs.PublicErrorInvalidInput, rejectDestination("source", "invalid import URL")))
	}
	if err := ValidateImportURL(u); err != nil {
		return PublicImportURL{}, catErrs.Unretriable(catErrs.Public(catErrs.PublicErrorInvalidInput, err))
	}
	return PublicImportURL{url: u}, nil
}

func (u PublicImportURL) String() string { return u.url.String() }

type ByteRange struct {
	Offset int64
	Length int64
}

func (r *ByteRange) header() (string, error) {
	if r == nil {
		return "", nil
	}
	if r.Offset < 0 || r.Length <= 0 || r.Offset > math.MaxInt64-r.Length+1 {
		return "", catErrs.Unretriable(fmt.Errorf("invalid import byte range"))
	}
	return fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1), nil
}

func (r *ByteRange) contentRangePrefix() string {
	return fmt.Sprintf("bytes %d-%d/", r.Offset, r.Offset+r.Length-1)
}

type FetchStream struct {
	Body         io.ReadCloser
	Size         int64
	RangeApplied bool
}

type FetchResult struct {
	Path         string
	Size         int64
	RangeApplied bool
}

func (r *FetchResult) Close() error {
	if r == nil {
		return nil
	}
	err := os.Remove(r.Path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

type PublicFetcher struct {
	HTTPClient *http.Client
	DStorage   *DStorageDownload
}

func (f PublicFetcher) Open(ctx context.Context, requestID string, source PublicImportURL, byteRange *ByteRange, maxBytes int64) (*FetchStream, error) {
	rangeHeader, err := byteRange.header()
	if err != nil {
		return nil, err
	}
	if maxBytes < 0 || (byteRange != nil && byteRange.Length > maxBytes) {
		return nil, importTooLargeError(maxBytes)
	}

	if IsDStorageResource(source.String()) {
		dStorage := f.DStorage
		if dStorage == nil {
			dStorage = NewDStorageDownload(f.HTTPClient)
		}
		body, err := dStorage.DownloadDStorageFromGatewayList(ctx, source.String(), requestID)
		if err != nil {
			err = catErrs.Public(catErrs.PublicErrorFileInaccessible, err)
		}
		return boundedFetchStream(body, -1, false, maxBytes, err)
	}

	switch strings.ToLower(source.url.Scheme) {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.String(), nil)
		if err != nil {
			return nil, catErrs.Unretriable(fmt.Errorf("invalid import request: %w", err))
		}
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := f.HTTPClient.Do(req)
		if err != nil {
			return nil, catErrs.Public(catErrs.PublicErrorFileInaccessible, fmt.Errorf("download error for import request: %w", err))
		}
		if err := classifyImportHTTPStatus(resp.StatusCode); err != nil {
			resp.Body.Close()
			return nil, err
		}
		rangeApplied := byteRange != nil && resp.StatusCode == http.StatusPartialContent
		if rangeApplied && !strings.HasPrefix(resp.Header.Get("Content-Range"), byteRange.contentRangePrefix()) {
			resp.Body.Close()
			return nil, catErrs.Unretriable(fmt.Errorf("import range response did not match request"))
		}
		return boundedFetchStream(resp.Body, resp.ContentLength, rangeApplied, maxBytes, nil)

	case "s3", "s3+http", "s3+https":
		fileInfo, err := getOSURL(ctx, source.String(), rangeHeader, importHTTPClient)
		rangeApplied := byteRange != nil
		if byteRange != nil && errors.Is(err, drivers.ErrNotSupported) {
			fileInfo, err = getOSURL(ctx, source.String(), "", importHTTPClient)
			rangeApplied = false
		}
		if err != nil {
			return nil, catErrs.Public(catErrs.PublicErrorFileInaccessible, fmt.Errorf("download error for import request: %w", err))
		}
		if rangeApplied && !strings.HasPrefix(fileInfo.ContentRange, byteRange.contentRangePrefix()) {
			fileInfo.Body.Close()
			return nil, catErrs.Unretriable(fmt.Errorf("import range response did not match request"))
		}
		size := int64(-1)
		if fileInfo.Size != nil {
			size = *fileInfo.Size
		}
		return boundedFetchStream(fileInfo.Body, size, rangeApplied, maxBytes, nil)
	default:
		return nil, catErrs.Unretriable(fmt.Errorf("unsupported import URL scheme"))
	}
}

func (f PublicFetcher) FetchToTemp(ctx context.Context, requestID string, source PublicImportURL, byteRange *ByteRange, maxBytes int64) (*FetchResult, error) {
	stream, err := f.Open(ctx, requestID, source, byteRange, maxBytes)
	if err != nil {
		return nil, err
	}
	return fetchStreamToTemp(stream, byteRange)
}

func fetchImportToTemp(ctx context.Context, requestID, rawURL string, maxBytes int64) (*FetchResult, error) {
	u, err := url.Parse(rawURL)
	if err == nil && isLocalObjectStoreURL(u) {
		body, err := DownloadOSURL(rawURL)
		if err != nil {
			return nil, err
		}
		stream, err := boundedFetchStream(body, -1, false, maxBytes, nil)
		if err != nil {
			return nil, err
		}
		return fetchStreamToTemp(stream, nil)
	}
	source, err := ParsePublicImportURL(rawURL)
	if err != nil {
		return nil, err
	}
	return (PublicFetcher{HTTPClient: retryableHttpClient}).FetchToTemp(ctx, requestID, source, nil, maxBytes)
}

func fetchStreamToTemp(stream *FetchStream, byteRange *ByteRange) (_ *FetchResult, err error) {
	defer stream.Body.Close()
	tmp, err := os.CreateTemp(os.TempDir(), "public-probe-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary import file: %w", err)
	}
	result := &FetchResult{Path: tmp.Name(), RangeApplied: stream.RangeApplied}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = result.Close()
		}
	}()
	result.Size, err = io.Copy(tmp, stream.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to write temporary import file: %w", err)
	}
	if byteRange != nil && stream.RangeApplied && result.Size != byteRange.Length {
		return nil, catErrs.Unretriable(fmt.Errorf("import range response had an unexpected size"))
	}
	if err = tmp.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temporary import file: %w", err)
	}
	return result, nil
}

func classifyImportHTTPStatus(status int) error {
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		return nil
	}
	msg := fmt.Sprintf("download error for import request: %d %s", status, strings.ToLower(http.StatusText(status)))
	if status == http.StatusNotFound {
		return catErrs.NewObjectNotFoundError(msg, nil)
	}
	if status < http.StatusInternalServerError {
		return catErrs.Unretriable(catErrs.Public(catErrs.PublicErrorFileInaccessible, errors.New(msg)))
	}
	return catErrs.Public(catErrs.PublicErrorFileInaccessible, errors.New(msg))
}

func classifyImportError(err error) error {
	var statusError interface{ StatusCode() int }
	if errors.As(err, &statusError) {
		return classifyImportHTTPStatus(statusError.StatusCode())
	}
	return err
}

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
	limit     int64
	exceeded  bool
}

func boundedFetchStream(body io.ReadCloser, size int64, rangeApplied bool, maxBytes int64, err error) (*FetchStream, error) {
	if err != nil {
		return nil, err
	}
	if size > maxBytes {
		body.Close()
		return nil, importTooLargeError(maxBytes)
	}
	return &FetchStream{Body: &boundedReadCloser{ReadCloser: body, remaining: maxBytes, limit: maxBytes}, Size: size, RangeApplied: rangeApplied}, nil
}

func (r *boundedReadCloser) Read(p []byte) (int, error) {
	if r.exceeded {
		return 0, importTooLargeError(r.limit)
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.ReadCloser.Read(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
		r.remaining = 0
		r.exceeded = true
		if n == 0 {
			return 0, importTooLargeError(r.limit)
		}
		return n, nil
	}
	r.remaining -= int64(n)
	return n, err
}

// DownloadFirstPublicHLSProbeToTemporary materializes a self-contained local
// playlist containing the first segment and any initialization section or key
// it requires. Every remote object is fetched through the guarded import
// client, while ffprobe is given only local paths.
func DownloadFirstPublicHLSProbeToTemporary(ctx context.Context, requestID, manifestURL string) (string, func(), float64, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return "", nil, 0, rejectDestination("source_probe", "invalid HLS manifest URL")
	}
	if err := ValidateImportURL(u); err != nil {
		return "", nil, 0, err
	}
	playlist, err := downloadRenditionManifest(ctx, requestID, manifestURL)
	if err != nil {
		return "", nil, 0, fmt.Errorf("failed to download HLS manifest for probing: %w", err)
	}
	if len(playlist.GetAllSegments()) == 0 {
		return "", nil, 0, fmt.Errorf("HLS manifest contains no segments")
	}
	filename, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(ctx, requestID, manifestURL, playlist, 0)
	if err != nil {
		return "", nil, 0, err
	}
	duration, _ := video.GetTotalDurationAndSegments(&playlist)
	return filename, cleanup, duration, nil
}

// DownloadPublicHLSSegmentProbeToTemporary creates a private temporary HLS
// package for one segment from an already-parsed playlist. The package has no
// remote references and is bounded independently of the full input size.
func DownloadPublicHLSSegmentProbeToTemporary(ctx context.Context, requestID, manifestURL string, playlist m3u8.MediaPlaylist, segmentIndex int) (filename string, cleanup func(), err error) {
	manifest, err := url.Parse(manifestURL)
	if err != nil {
		return "", nil, rejectDestination("source_probe", "invalid HLS manifest URL")
	}
	if err := ValidateImportURL(manifest); err != nil {
		return "", nil, err
	}

	mediaSegments := playlist.GetAllSegments()
	if segmentIndex < 0 || segmentIndex >= len(mediaSegments) || mediaSegments[segmentIndex] == nil {
		return "", nil, catErrs.Unretriable(fmt.Errorf("invalid HLS probe segment index"))
	}
	segmentURLs, err := GetSourceSegmentURLs(manifestURL, playlist)
	if err != nil {
		return "", nil, fmt.Errorf("failed to validate HLS probe segments: %w", err)
	}

	key, initMap := effectiveHLSProbeDependencies(playlist, segmentIndex)
	var keyURL, mapURL *url.URL
	if key != nil && !strings.EqualFold(key.Method, "NONE") {
		if err := validateHLSProbeKey(key); err != nil {
			return "", nil, err
		}
		keyURL, err = resolvePublicHLSProbeReference(manifestURL, key.URI, "key")
		if err != nil {
			return "", nil, err
		}
	}
	if initMap != nil {
		mapURL, err = resolvePublicHLSProbeReference(manifestURL, initMap.URI, "initialization section")
		if err != nil {
			return "", nil, err
		}
	}

	probeDir, err := os.MkdirTemp(os.TempDir(), "public-hls-probe-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temporary HLS probe directory: %w", err)
	}
	removeProbeDir := func() { _ = os.RemoveAll(probeDir) }
	defer func() {
		if err != nil {
			removeProbeDir()
			filename = ""
			cleanup = nil
		}
	}()

	remaining := MaxTemporaryHLSProbeSizeBytes
	type stagedObject struct {
		localName string
		ranged    bool
	}
	staged := make(map[string]stagedObject)
	stage := func(remote *url.URL, localName string, objectLimit, offset, length int64) (string, bool, error) {
		cacheKey := fmt.Sprintf("%s|%d|%d", remote.String(), offset, length)
		if existing, ok := staged[cacheKey]; ok {
			return existing.localName, existing.ranged, nil
		}
		limit := min(remaining, objectLimit)
		if limit <= 0 {
			return "", false, importTooLargeError(MaxTemporaryHLSProbeSizeBytes)
		}
		source, fetchErr := ParsePublicImportURL(remote.String())
		if fetchErr != nil {
			return "", false, fetchErr
		}
		var byteRange *ByteRange
		if length > 0 {
			byteRange = &ByteRange{Offset: offset, Length: length}
		}
		result, fetchErr := (PublicFetcher{HTTPClient: retryableHttpClient}).FetchToTemp(ctx, requestID, source, byteRange, limit)
		if fetchErr != nil {
			return "", false, fetchErr
		}
		defer result.Close()
		if result.Size > remaining {
			return "", false, importTooLargeError(MaxTemporaryHLSProbeSizeBytes)
		}
		if renameErr := os.Rename(result.Path, filepath.Join(probeDir, localName)); renameErr != nil {
			return "", false, fmt.Errorf("failed to stage temporary HLS probe object: %w", renameErr)
		}
		remaining -= result.Size
		staged[cacheKey] = stagedObject{localName: localName, ranged: result.RangeApplied}
		return localName, result.RangeApplied, nil
	}

	localKey := ""
	if keyURL != nil {
		localKey, _, err = stage(keyURL, "key.bin", MaxTemporaryHLSKeySizeBytes, 0, 0)
		if err != nil {
			return "", nil, fmt.Errorf("failed to stage HLS probe key: %w", err)
		}
	}
	localMap := ""
	mapRanged := false
	if mapURL != nil {
		localMap, mapRanged, err = stage(mapURL, localHLSProbeName("init", mapURL, ".mp4"), remaining, initMap.Offset, initMap.Limit)
		if err != nil {
			return "", nil, fmt.Errorf("failed to stage HLS probe initialization section: %w", err)
		}
	}
	segmentURL := segmentURLs[segmentIndex].URL
	segmentOffset := effectiveHLSByteRangeOffset(mediaSegments, segmentIndex)
	localSegment, segmentRanged, err := stage(segmentURL, localHLSProbeName("segment", segmentURL, ".ts"), remaining, segmentOffset, mediaSegments[segmentIndex].Limit)
	if err != nil {
		return "", nil, fmt.Errorf("failed to stage HLS probe segment: %w", err)
	}

	localManifest, err := encodeLocalHLSProbeManifest(mediaSegments, segmentIndex, localSegment, segmentRanged, key, localKey, initMap, localMap, mapRanged, playlist.Iframe)
	if err != nil {
		return "", nil, err
	}
	manifestPath := filepath.Join(probeDir, "index.m3u8")
	if err = os.WriteFile(manifestPath, []byte(localManifest), 0o600); err != nil {
		return "", nil, fmt.Errorf("failed to write temporary HLS probe manifest: %w", err)
	}
	return manifestPath, removeProbeDir, nil
}

func effectiveHLSProbeDependencies(playlist m3u8.MediaPlaylist, segmentIndex int) (*m3u8.Key, *m3u8.Map) {
	// The decoder also copies the first key/map it encounters to the playlist
	// defaults, even when that tag appears after earlier segments. Walking only
	// segment-scoped changes preserves the actual timeline semantics.
	var key *m3u8.Key
	var initMap *m3u8.Map
	for i, segment := range playlist.GetAllSegments() {
		if i > segmentIndex {
			break
		}
		if segment == nil {
			continue
		}
		if segment.Key != nil {
			key = segment.Key
		}
		if segment.Map != nil {
			initMap = segment.Map
		}
	}
	return key, initMap
}

func resolvePublicHLSProbeReference(manifestURL, reference, kind string) (*url.URL, error) {
	if reference == "" {
		return nil, catErrs.Unretriable(fmt.Errorf("HLS probe %s URI is empty", kind))
	}
	resolved, err := ManifestURLToSegmentURL(manifestURL, reference)
	if err != nil {
		return nil, catErrs.Unretriable(fmt.Errorf("invalid HLS probe %s URI", kind))
	}
	if err := ValidateImportURL(resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

func validateHLSProbeKey(key *m3u8.Key) error {
	if key.Method == "" || !isHLSProbeToken(key.Method) {
		return catErrs.Unretriable(fmt.Errorf("HLS probe key method is invalid"))
	}
	for _, value := range []string{key.Keyformat, key.Keyformatversions} {
		if strings.ContainsAny(value, "\"\r\n") {
			return catErrs.Unretriable(fmt.Errorf("HLS probe key metadata is invalid"))
		}
	}
	if strings.ContainsAny(key.IV, ",\r\n") {
		return catErrs.Unretriable(fmt.Errorf("HLS probe key IV is invalid"))
	}
	return nil
}

func isHLSProbeToken(value string) bool {
	for _, char := range value {
		if (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}

func localHLSProbeName(prefix string, remote *url.URL, fallbackExtension string) string {
	extension := filepath.Ext(remote.Path)
	if len(extension) < 2 || len(extension) > 10 {
		extension = fallbackExtension
	} else {
		for _, char := range extension[1:] {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
				extension = fallbackExtension
				break
			}
		}
	}
	return prefix + strings.ToLower(extension)
}

func encodeLocalHLSProbeManifest(segments []*m3u8.MediaSegment, segmentIndex int, localSegment string, segmentRanged bool, key *m3u8.Key, localKey string, initMap *m3u8.Map, localMap string, mapRanged, iframe bool) (string, error) {
	segment := segments[segmentIndex]
	targetDuration := int64(math.Ceil(segment.Duration))
	if targetDuration < 1 {
		targetDuration = 1
	}

	var manifest strings.Builder
	manifest.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	fmt.Fprintf(&manifest, "#EXT-X-MEDIA-SEQUENCE:%d\n", segment.SeqId)
	fmt.Fprintf(&manifest, "#EXT-X-TARGETDURATION:%d\n", targetDuration)
	if iframe {
		manifest.WriteString("#EXT-X-I-FRAMES-ONLY\n")
	}
	if key != nil && !strings.EqualFold(key.Method, "NONE") {
		if localKey == "" {
			return "", catErrs.Unretriable(fmt.Errorf("local HLS probe key is missing"))
		}
		fmt.Fprintf(&manifest, "#EXT-X-KEY:METHOD=%s,URI=\"%s\"", key.Method, localKey)
		if key.IV != "" {
			fmt.Fprintf(&manifest, ",IV=%s", key.IV)
		}
		if key.Keyformat != "" {
			fmt.Fprintf(&manifest, ",KEYFORMAT=\"%s\"", key.Keyformat)
		}
		if key.Keyformatversions != "" {
			fmt.Fprintf(&manifest, ",KEYFORMATVERSIONS=\"%s\"", key.Keyformatversions)
		}
		manifest.WriteByte('\n')
	}
	if initMap != nil {
		if localMap == "" {
			return "", catErrs.Unretriable(fmt.Errorf("local HLS probe initialization section is missing"))
		}
		fmt.Fprintf(&manifest, "#EXT-X-MAP:URI=\"%s\"", localMap)
		if initMap.Limit > 0 && !mapRanged {
			fmt.Fprintf(&manifest, ",BYTERANGE=\"%d@%d\"", initMap.Limit, initMap.Offset)
		}
		manifest.WriteByte('\n')
	}
	if segment.Limit > 0 && !segmentRanged {
		fmt.Fprintf(&manifest, "#EXT-X-BYTERANGE:%d@%d\n", segment.Limit, effectiveHLSByteRangeOffset(segments, segmentIndex))
	}
	fmt.Fprintf(&manifest, "#EXTINF:%s,\n%s\n#EXT-X-ENDLIST\n", strconv.FormatFloat(segment.Duration, 'f', -1, 64), localSegment)
	return manifest.String(), nil
}

func effectiveHLSByteRangeOffset(segments []*m3u8.MediaSegment, segmentIndex int) int64 {
	segment := segments[segmentIndex]
	if segment.Offset != 0 || segmentIndex == 0 {
		return segment.Offset
	}
	previous := segments[segmentIndex-1]
	if previous == nil || previous.Limit == 0 || previous.URI != segment.URI {
		return 0
	}
	return effectiveHLSByteRangeOffset(segments, segmentIndex-1) + previous.Limit
}

func importTooLargeError(maxBytes int64) error {
	return catErrs.Unretriable(catErrs.Public(catErrs.PublicErrorInvalidInput, fmt.Errorf("import exceeded maximum size of %d bytes", maxBytes)))
}
