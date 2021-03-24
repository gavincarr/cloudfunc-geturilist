/*
Google Cloud Function to do HTTP GETs on a series of URLs from a text/uri-list,
and capture the responses.

Processes a list of urls contained in a Google Cloud Storage object,
fetches the content at each URL (following redirects), and saves the (final)
response to a Google Cloud Storage Bucket, in gzipped WARC format, with one
response object per requested URL. Objects can be named in various ways, but
default to using the SHA1 hash of the requested URL.

Configuration environment variables:
- GUL_OUTPUT_BUCKET - GCS Bucket name to write to (required)
- GUL_NAME_FORMAT - GCS Object name format, one of:
  - "sha1" - output filename is the sha1 hash of the requested url (default)
  - "url" - output filename is the (path-escaped) requested url
  - "hostname" - output filename is the hostname from the requested url
    (but beware of collisions!)
- GUL_CONCURRENCY - how many requests to have in flight concurrently (default: 3)
- GUL_SLEEP_SECONDS - how long to sleep between requests (float, default 0.0;
  required primarily if you're hitting the same server repeatedly, to be polite)
*/

package cfgul

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gavincarr/warc"
)

// Config defaults
const (
	GUL_NAME_FORMAT   = "sha1"
	GUL_CONCURRENCY   = 3
	GUL_SLEEP_SECONDS = 0.0
)

// Config holds our configuration settings
type Config struct {
	OutputBucket string
	NameFormat   string
	Concurrency  int
	SleepSeconds float64
}

// GCSEvent is the payload of a GCS finalize event
type GCSEvent struct {
	Bucket         string    `json:"bucket"`
	Name           string    `json:"name"`
	Metageneration string    `json:"metageneration"`
	ResourceState  string    `json:"resourceState"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
}

// newConfig returns a Config object based on the environment and defaults
func newConfig() Config {
	config := Config{
		NameFormat:   GUL_NAME_FORMAT,
		Concurrency:  GUL_CONCURRENCY,
		SleepSeconds: GUL_SLEEP_SECONDS,
	}
	config.OutputBucket = os.Getenv("GUL_OUTPUT_BUCKET")
	if config.OutputBucket == "" {
		log.Fatal("GUL_OUTPUT_BUCKET not set in environment - aborting")
	}
	if nameFormat := os.Getenv("GUL_NAME_FORMAT"); nameFormat != "" {
		if nameFormat != "sha1" && nameFormat != "url" && nameFormat != "hostname" {
			log.Printf("Warning: invalid GUL_NAME_FORMAT value %q - using default %q\n",
				nameFormat, GUL_NAME_FORMAT)
		} else {
			config.NameFormat = nameFormat
		}
	}
	if concurrencyStr := os.Getenv("GUL_CONCURRENCY"); concurrencyStr != "" {
		concurrency, err := strconv.Atoi(concurrencyStr)
		if err != nil {
			log.Fatalf("parsing GUL_CONCURRENCY %q: %s\n", concurrencyStr, err)
		}
		log.Printf("Concurrency: %d\n", concurrency)
		config.Concurrency = concurrency
	}
	if sleepSecStr := os.Getenv("GUL_SLEEP_SECONDS"); sleepSecStr != "" {
		sleepSeconds, err := strconv.ParseFloat(sleepSecStr, 64)
		if err != nil {
			log.Fatalf("parsing GUL_SLEEP_SECONDS %q: %s\n", sleepSecStr, err)
		}
		log.Printf("Sleep seconds: %f\n", sleepSeconds)
		config.SleepSeconds = sleepSeconds
	}
	return config
}

// parsePrefix splits name on '/' characters, and, if len(tokens) > 1,
// returns all but the last token, rejoined with '/'.
func parsePrefix(name string) string {
	tokens := strings.Split(name, "/")
	if len(tokens) == 1 {
		return ""
	}
	return strings.Join(tokens[:len(tokens)-1], "/")
}

// fetchUrls reads the input bucket object called name, and returns a slice of the *url.URLs
// it contains. Invalid URLs (parse failures) are skipped/dropped.
func fetchUrls(ctx context.Context, bucket *storage.BucketHandle, name string) []*neturl.URL {
	obj := bucket.Object(name)

	rdr, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatalf("creating reader for object %q: %s\n", name, err)
	}
	defer rdr.Close()

	var scanner *bufio.Scanner
	reGzip := regexp.MustCompile(`\.gz$`)
	if reGzip.MatchString(name) {
		grdr, err := gzip.NewReader(rdr)
		if err != nil {
			log.Fatalf("creating gzip reader for object %q: %s\n", name, err)
		}
		defer grdr.Close()
		scanner = bufio.NewScanner(grdr)
	} else {
		scanner = bufio.NewScanner(rdr)
	}

	var urls []*neturl.URL
	for scanner.Scan() {
		urlStr := scanner.Text()
		url, err := neturl.Parse(urlStr)
		if err != nil {
			log.Printf("Warning: skipping invalid url: %s\n", err)
			continue
		}
		urls = append(urls, url)
	}

	return urls
}

// objectName returns the appropriate object filename for url
func objectName(nameFormat string, url *neturl.URL) string {
	var filename string
	switch nameFormat {
	case "sha1":
		filename = fmt.Sprintf("%x", sha1.Sum([]byte(url.String())))
	case "url":
		filename = neturl.PathEscape(url.String())
	case "hostname":
		filename = url.Hostname()
	default:
		// We've already checked validity, so this should never happen
		log.Fatalf("invalid NameFormat %q", nameFormat)
	}
	return filename + ".warc.gz"
}

func wrapBufferWARC(content *bytes.Buffer, urlstr string) (*bytes.Buffer, error) {
	b := bytes.Buffer{}

	gzwriter := gzip.NewWriter(&b)
	writer := warc.NewWriter(gzwriter)

	record := warc.NewRecord()
	record.Header.Set("warc-type", "response")
	record.Header.Set("content-type", "application/http;msgtype=response")
	record.Header.Set("warc-target-uri", urlstr)
	record.Content = content

	if _, err := writer.WriteRecord(record); err != nil {
		return nil, err
	}
	if err := gzwriter.Close(); err != nil {
		return nil, err
	}

	return &b, nil
}

// getHTTP does a GET on url and returns a buffer with the final response in WARC format.
// Errors (whether HTTP or connection errors) are captured as HTTP headers.
func getHTTP(reqCtx context.Context, client *http.Client, url *neturl.URL) (*bytes.Buffer, error) {
	content := bytes.Buffer{}

	urlStr := url.String()
	//log.Printf("++ doing GET for %q\n", urlStr)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		// Request error
		content.WriteString("HTTP/1.0 599 Request Error\r\n")
		content.WriteString("Error: " + err.Error() + "\r\n")
		content.WriteString("\r\n")
		return wrapBufferWARC(&content, urlStr)
	}
	resp, err := client.Do(req.WithContext(reqCtx))
	if err != nil {
		// Connection error
		content.WriteString("HTTP/1.0 599 Connection Error\r\n")
		content.WriteString("Error: " + err.Error() + "\r\n")
		content.WriteString("\r\n")
		return wrapBufferWARC(&content, urlStr)
	}
	defer resp.Body.Close()

	err = resp.Write(&content)
	if err != nil {
		return nil, err
	}

	return wrapBufferWARC(&content, urlStr)
}

// saveObject writes data to a GCS object called name
func saveObject(ctx context.Context, bucket *storage.BucketHandle, prefix, name string, data *bytes.Buffer) {
	fullname := name
	if prefix != "" {
		fullname = prefix + "/" + name
	}
	obj := bucket.Object(fullname)
	w := obj.NewWriter(ctx)
	defer w.Close()
	io.Copy(w, data)
}

// deleteObject deletes the (input) bucket GCS object called name (on run completion)
func deleteObject(ctx context.Context, bucket *storage.BucketHandle, name string) {
	obj := bucket.Object(name)
	err := obj.Delete(ctx)
	if err != nil {
		log.Fatalf("deleting object %q failed: %s\n", name, err)
	}
}

// GetURIList is our GCS Cloud Function entrypoint.
func GetURIList(ctx context.Context, e GCSEvent) error {
	log.SetFlags(0)
	log.Printf("%s execution started\n", e.Name)

	// Only handle objects that end in '.txt(.gz)?'
	reTxt := regexp.MustCompile(`.txt(\.gz)?$`)
	if !reTxt.MatchString(e.Name) {
		log.Printf("skipping non-uri file %q\n", e.Name)
		return nil
	}

	config := newConfig()
	prefix := parsePrefix(e.Name)

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("instantiating storage client: %s\n", err)
	}

	bucketIn := storageClient.Bucket(e.Bucket)
	bucketOut := storageClient.Bucket(config.OutputBucket)

	// Fetch the input object and parse as a text/uri-list
	urls := fetchUrls(ctx, bucketIn, e.Name)
	log.Printf("URL count: %d\n", len(urls))

	//httpClient := http.Client{Timeout: 10 * time.Second}
	httpClient := http.Client{}
	sem := make(chan struct{}, config.Concurrency)
	sleep := time.Duration(config.SleepSeconds) * time.Second
	line := 0
	for _, url := range urls {
		name := objectName(config.NameFormat, url)

		if line%100 == 0 {
			log.Printf("%s [%d] %s\n", e.Name, line, url.String())
		}

		// Blocks until a sem slot is available
		sem <- struct{}{}
		go func(name string, url *neturl.URL) {
			defer func() { <-sem }() // Release our sem slot
			reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			// getHTTP almost always returns success - most errors are captured in data
			data, err := getHTTP(reqCtx, &httpClient, url)
			if err != nil {
				// Non-connection errors (e.g. WARC writes?) - log and give up
				log.Printf("getHTTP error: %s\n", err)
				return
			}
			//log.Printf("++ got result for %q, saving\n", url.String())
			// Note that we use ctx here, not reqCtx, as we save even on reqCtx timeout
			saveObject(ctx, bucketOut, prefix, name, data)
		}(name, url)

		line++
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

	// Wait until all clients have finished i.e. when the clients channel is full again
	for len(sem) > 0 {
		time.Sleep(1 * time.Second)
	}
	log.Printf("%s all clients completed, cleaning up\n", e.Name)

	// On completion, delete our url input object
	deleteObject(ctx, bucketIn, e.Name)

	log.Printf("%s execution completed\n", e.Name)
	return nil
}
