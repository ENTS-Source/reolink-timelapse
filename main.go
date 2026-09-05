package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	captureAttempts   = 3
	captureTimeout    = 20 * time.Second
	uploadTimeout     = 45 * time.Second
	maxPendingPerRun  = 5
	maxSnapshotSize   = 32 * 1024 * 1024 // 32 MiB
	defaultWorkingDir = "/var/lib/camera-timelapse"
	defaultTimezone   = "America/Edmonton"
	defaultS3Region   = "us-east-1"
)

type appConfig struct {
	cameraBaseURL     string
	cameraUser        string
	cameraPassword    string
	cameraInsecureTLS bool

	timezone string
	workDir  string

	s3Endpoint  string
	s3Region    string
	s3Bucket    string
	s3AccessKey string
	s3SecretKey string
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	loc, err := time.LoadLocation(cfg.timezone)
	if err != nil {
		log.Fatalf("load timezone %q: %v", cfg.timezone, err)
	}

	if err := os.MkdirAll(cfg.workDir, 0700); err != nil {
		log.Fatalf("create working directory: %v", err)
	}

	s3Client, err := newS3Client(cfg)
	if err != nil {
		log.Fatalf("configure S3 client: %v", err)
	}

	// Capture first. This means old pending uploads can never delay the
	// scheduled camera capture.
	now := time.Now()

	// Unix milliseconds are first so lexical filename order is always
	// chronological, including across DST fall-back in America/Edmonton.
	//
	// Example:
	// 1788465300123_20260903_091500_MDT.jpg
	filename := fmt.Sprintf(
		"%013d_%s.jpg",
		now.UnixMilli(),
		now.In(loc).Format("20060102_150405_MST"),
	)

	imagePath := filepath.Join(cfg.workDir, filename)

	width, height, size, err := captureWithRetry(cfg, imagePath)
	if err != nil {
		log.Fatalf("capture failed: %v", err)
	}

	log.Printf(
		"captured %s (%dx%d, %d bytes)",
		filename,
		width,
		height,
		size,
	)

	if width != 2560 || height != 1920 {
		log.Printf(
			"WARNING: snapshot is %dx%d; expected the RLC-520A mainstream to normally be 2560x1920",
			width,
			height,
		)
	}

	// Upload this new capture first, followed by a bounded number of
	// previously failed uploads. Bounding the queue means an S3 outage
	// can't make one cron invocation run for 15+ minutes.
	queue, err := uploadQueue(cfg.workDir, imagePath, maxPendingPerRun)
	if err != nil {
		log.Fatalf("build upload queue: %v", err)
	}

	for _, path := range queue {
		if err := uploadAndVerify(s3Client, cfg, path); err != nil {
			log.Printf(
				"upload failed for %s; leaving local copy for a later run: %v",
				filepath.Base(path),
				err,
			)
			continue
		}

		if err := os.Remove(path); err != nil {
			log.Printf(
				"uploaded %s but could not remove local copy: %v",
				filepath.Base(path),
				err,
			)
			continue
		}

		log.Printf("uploaded and removed local copy: %s", filepath.Base(path))
	}
}

func loadConfig() (appConfig, error) {
	cfg := appConfig{
		cameraBaseURL:     strings.TrimRight(os.Getenv("CAMERA_BASE_URL"), "/"),
		cameraUser:        os.Getenv("CAMERA_USER"),
		cameraPassword:    os.Getenv("CAMERA_PASSWORD"),
		cameraInsecureTLS: envBool("CAMERA_INSECURE_TLS", false),

		timezone: envDefault("TIMELAPSE_TIMEZONE", defaultTimezone),
		workDir:  envDefault("TIMELAPSE_WORK_DIR", defaultWorkingDir),

		s3Endpoint:  strings.TrimRight(os.Getenv("S3_ENDPOINT"), "/"),
		s3Region:    envDefault("S3_REGION", defaultS3Region),
		s3Bucket:    os.Getenv("S3_BUCKET"),
		s3AccessKey: os.Getenv("S3_ACCESS_KEY"),
		s3SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	required := map[string]string{
		"CAMERA_BASE_URL": cfg.cameraBaseURL,
		"CAMERA_USER":     cfg.cameraUser,
		"CAMERA_PASSWORD": cfg.cameraPassword,
		"S3_ENDPOINT":     cfg.s3Endpoint,
		"S3_BUCKET":       cfg.s3Bucket,
		"S3_ACCESS_KEY":   cfg.s3AccessKey,
		"S3_SECRET_KEY":   cfg.s3SecretKey,
	}

	for name, value := range required {
		if value == "" {
			return cfg, fmt.Errorf("required environment variable %s is empty", name)
		}
	}

	u, err := url.Parse(cfg.cameraBaseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return cfg, fmt.Errorf("invalid CAMERA_BASE_URL %q", cfg.cameraBaseURL)
	}

	return cfg, nil
}

func newS3Client(cfg appConfig) (*s3.Client, error) {
	ctx := context.Background()

	sdkCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.s3Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.s3AccessKey,
				cfg.s3SecretKey,
				"",
			),
		),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(sdkCfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(cfg.s3Endpoint)

		// DigitalOcean Spaces uses virtual-hosted-style addressing:
		// bucket.region.digitaloceanspaces.com
		options.UsePathStyle = false
	})

	return client, nil
}

func captureWithRetry(cfg appConfig, outputPath string) (int, int, int64, error) {
	var lastErr error

	for attempt := 1; attempt <= captureAttempts; attempt++ {
		width, height, size, err := captureSnapshot(cfg, outputPath)
		if err == nil {
			return width, height, size, nil
		}

		lastErr = err

		log.Printf(
			"capture attempt %d/%d failed: %v",
			attempt,
			captureAttempts,
			err,
		)

		if attempt < captureAttempts {
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}
	}

	return 0, 0, 0, lastErr
}

func captureSnapshot(cfg appConfig, outputPath string) (int, int, int64, error) {
	u, err := url.Parse(cfg.cameraBaseURL)
	if err != nil {
		return 0, 0, 0, err
	}

	u.Path = "/cgi-bin/api.cgi"

	q := u.Query()
	q.Set("cmd", "Snap")
	q.Set("channel", "0")
	q.Set("rs", strconv.FormatInt(time.Now().UnixNano(), 36))
	q.Set("user", cfg.cameraUser)
	q.Set("password", cfg.cameraPassword)
	u.RawQuery = q.Encode()

	transport := http.DefaultTransport.(*http.Transport).Clone()

	if cfg.cameraInsecureTLS {
		// Only enable this for a camera on a trusted LAN when it uses a
		// self-signed HTTPS certificate.
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, // intentionally configurable
		}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   captureTimeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), captureTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, 0, 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer resp.Body.Close()

	log.Println(resp.Header.Get("Content-Type"))
	if resp.Header.Get("Content-Type") != "image/jpeg" {
		b, _ := io.ReadAll(resp.Body)
		log.Println(string(b))
		return 0, 0, 0, fmt.Errorf("camera returned unexpected Content-Type: %s", resp.Header.Get("Content-Type"))
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, 0, 0, fmt.Errorf(
			"camera returned HTTP %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	partPath := outputPath + ".part"

	f, err := os.OpenFile(
		partPath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return 0, 0, 0, err
	}

	n, copyErr := io.Copy(
		f,
		io.LimitReader(resp.Body, maxSnapshotSize+1),
	)
	closeErr := f.Close()

	if copyErr != nil {
		os.Remove(partPath)
		return 0, 0, 0, copyErr
	}

	if closeErr != nil {
		os.Remove(partPath)
		return 0, 0, 0, closeErr
	}

	if n > maxSnapshotSize {
		os.Remove(partPath)
		return 0, 0, 0, fmt.Errorf("snapshot exceeds maximum expected size")
	}

	// Confirm that the camera actually returned a decodable JPEG rather
	// than an HTTP 200 response containing a CGI error message.
	checkFile, err := os.Open(partPath)
	if err != nil {
		os.Remove(partPath)
		return 0, 0, 0, err
	}

	imageCfg, err := jpeg.DecodeConfig(checkFile)
	checkFile.Close()

	if err != nil {
		os.Remove(partPath)
		return 0, 0, 0, fmt.Errorf("camera response is not a valid JPEG: %w", err)
	}

	if err := os.Rename(partPath, outputPath); err != nil {
		os.Remove(partPath)
		return 0, 0, 0, err
	}

	return imageCfg.Width, imageCfg.Height, n, nil
}

func uploadAndVerify(client *s3.Client, cfg appConfig, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	key := filepath.Base(path)

	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(cfg.s3Bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentType:   aws.String("image/jpeg"),
		ContentLength: aws.Int64(stat.Size()),
	})
	if err != nil {
		return fmt.Errorf("PutObject: %w", err)
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.s3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("HeadObject verification: %w", err)
	}

	if head.ContentLength == nil {
		return fmt.Errorf("HeadObject returned no Content-Length")
	}

	if *head.ContentLength != stat.Size() {
		return fmt.Errorf(
			"uploaded size mismatch: local=%d remote=%d",
			stat.Size(),
			*head.ContentLength,
		)
	}

	return nil
}

func uploadQueue(workDir, current string, maximum int) ([]string, error) {
	all, err := filepath.Glob(filepath.Join(workDir, "*.jpg"))
	if err != nil {
		return nil, err
	}

	sort.Strings(all)

	queue := make([]string, 0, maximum)

	// Always prioritize the capture from this invocation.
	queue = append(queue, current)

	for _, path := range all {
		if path == current {
			continue
		}

		queue = append(queue, path)

		if len(queue) >= maximum {
			break
		}
	}

	return queue, nil
}

func envDefault(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func envBool(name string, defaultValue bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}

	result, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("%s must be a boolean: %v", name, err)
	}

	return result
}
