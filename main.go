package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dustin/go-humanize"
)

type (
	pathCfg struct {
		IncludeFiles   []string `json:"includeFiles"`
		includeFiles   []*regexp.Regexp
		ExcludeFiles   []string `json:"excludeFiles"`
		excludeFiles   []*regexp.Regexp
		IncludeFolders []string `json:"includeFolders"`
		includeFolders []*regexp.Regexp
		ExcludeFolders []string `json:"excludeFolders"`
		excludeFolders []*regexp.Regexp
	}

	configuration struct {
		Bucket string             `json:"bucket"`
		Paths  map[string]pathCfg `json:"paths"`
	}

	flags struct {
		run   bool
		prune bool
	}

	runner struct {
		ctx          context.Context
		flags        flags
		config       *configuration
		metadataFile string
		metadata     map[string]time.Time
		processed    []string
		host         string
		client       *s3.Client
	}
)

const (
	metadataFile = "metadata.json"
	regexFormat  = "(?sm)^%s$"
)

func main() {
	r := newRunner()
	r.run()
}

func newRunner() *runner {
	ctx := context.Background()
	ud, err := os.UserHomeDir()
	checkError(err)
	bnp := path.Join(ud, ".backup")
	bd, err := os.Open(bnp)
	if errors.Is(err, os.ErrNotExist) {
		err = os.Mkdir(bnp, 0750)
		checkError(err)
		bd, err = os.Open(bnp)
	}
	checkError(err)
	err = bd.Close()
	checkError(err)
	var host string
	var cl *s3.Client
	r := slices.Contains(os.Args, "-run")
	p := slices.Contains(os.Args, "-prune")
	if r || p {
		host, err = os.Hostname()
		checkError(err)
		awsCfg, err := config.LoadDefaultConfig(ctx)
		checkError(err)
		cl = s3.NewFromConfig(awsCfg)
	}
	mfn := path.Join(bnp, metadataFile)
	return &runner{
		ctx: ctx,
		flags: flags{
			run:   r,
			prune: p,
		},
		config:       loadConfiguration(bnp),
		metadataFile: mfn,
		metadata:     loadMetadata(mfn),
		processed:    []string{},
		host:         host,
		client:       cl,
	}
}

func loadConfiguration(bnp string) *configuration {
	cfg := &configuration{
		Paths: map[string]pathCfg{},
	}
	cfn := path.Join(bnp, "config.json")
	cf, err := os.ReadFile(cfn)
	if errors.Is(err, os.ErrNotExist) {
		var b []byte
		b, err = json.Marshal(cfg)
		checkError(err)
		err = os.WriteFile(cfn, b, 0666)
		checkError(err)
		cf, err = os.ReadFile(cfn)
	}
	checkError(err)
	err = json.Unmarshal(cf, cfg)
	checkError(err)
	compile(cfg)
	return cfg
}

func compile(cfg *configuration) {
	for k, c := range cfg.Paths {
		for _, i := range c.IncludeFiles {
			c.includeFiles = append(c.includeFiles, regexp.MustCompile(fmt.Sprintf(regexFormat, i)))
		}
		for _, i := range c.ExcludeFiles {
			c.excludeFiles = append(c.excludeFiles, regexp.MustCompile(fmt.Sprintf(regexFormat, i)))
		}
		for _, i := range c.IncludeFolders {
			c.includeFolders = append(c.includeFolders, regexp.MustCompile(fmt.Sprintf(regexFormat, i)))
		}
		for _, i := range c.ExcludeFolders {
			c.excludeFolders = append(c.excludeFolders, regexp.MustCompile(fmt.Sprintf(regexFormat, i)))
		}
		cfg.Paths[k] = c
	}
}

func loadMetadata(mfn string) map[string]time.Time {
	meta := make(map[string]time.Time)
	mf, err := os.ReadFile(mfn)
	if errors.Is(err, os.ErrNotExist) {
		var b []byte
		b, err = json.Marshal(meta)
		checkError(err)
		err = os.WriteFile(mfn, b, 0666)
		checkError(err)
		mf, err = os.ReadFile(mfn)
	}
	checkError(err)
	err = json.Unmarshal(mf, &meta)
	checkError(err)
	return meta
}

func (r *runner) run() {
	var ts uint64
	var keys []string
	for k := range r.config.Paths {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		ts += r.execute(k, true, r.config.Paths[k])
	}
	ts += r.execute(r.metadataFile, true, pathCfg{
		includeFiles: []*regexp.Regexp{
			regexp.MustCompile(fmt.Sprintf(regexFormat, "metadata\\.json")),
		},
		includeFolders: []*regexp.Regexp{
			regexp.MustCompile(fmt.Sprintf(regexFormat, "\\.backup")),
		},
	})
	if r.flags.prune {
		slices.Sort(r.processed)
		for k := range r.metadata {
			if !slices.Contains(r.processed, k) {
				r.prune(k)
			}
		}
	}
	fmt.Println("Total size:", humanize.Bytes(ts))
}

func (r *runner) execute(filePath string, force bool, cfg pathCfg) uint64 {
	f, err := os.Open(filePath)
	checkError(err)
	defer func() {
		err = f.Close()
		checkError(err)
	}()
	s, err := f.Stat()
	checkError(err)
	if s.IsDir() {
		return r.executeDir(filePath, force, cfg, s, f)
	}
	return r.executeFile(filePath, cfg, s, f)
}

func (r *runner) executeDir(filePath string, force bool, cfg pathCfg, s os.FileInfo, f *os.File) uint64 {
	if !force && len(cfg.includeFolders) > 0 && !match(s.Name(), cfg.includeFolders) {
		return 0
	}
	if !force && match(s.Name(), cfg.excludeFolders) {
		return 0
	}
	if slices.Contains(r.processed, filePath) {
		panic("Already processed: " + filePath)
	}
	r.processed = append(r.processed, filePath)
	var fs []os.FileInfo
	fs, err := f.Readdir(0)
	checkError(err)
	var ts uint64
	for _, fi := range fs {
		ts += r.execute(path.Join(filePath, fi.Name()), false, cfg)
	}
	if ts > 0 {
		fmt.Println(filePath, "size:", humanize.Bytes(ts))
	}
	return ts
}

func (r *runner) executeFile(filePath string, cfg pathCfg, s os.FileInfo, f *os.File) uint64 {
	if len(cfg.includeFiles) > 0 && !match(s.Name(), cfg.includeFiles) {
		return 0
	}
	if match(s.Name(), cfg.excludeFiles) {
		return 0
	}
	if slices.Contains(r.processed, filePath) {
		panic("Already processed: " + filePath)
	}
	r.processed = append(r.processed, filePath)
	mt, ok := r.metadata[f.Name()]
	if ok && mt.Equal(s.ModTime()) {
		return 0
	}
	r.backup(filePath, f, s)
	return uint64(s.Size())
}

func (r *runner) backup(filePath string, f *os.File, s os.FileInfo) {
	if !r.flags.run {
		r.plan(filePath, s)
		return
	}
	fs := uint64(s.Size())
	fmt.Println("Backing up", filePath, "with", humanize.Bytes(fs), "...")
	_, err := r.client.PutObject(r.ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(r.host + filePath),
		Body:   f,
	})
	checkError(err)
	if filePath != r.metadataFile {
		r.metadata[filePath] = s.ModTime()
		r.saveMetadata()
	}
}

func (r *runner) saveMetadata() {
	b, err := json.Marshal(r.metadata)
	checkError(err)
	err = os.WriteFile(r.metadataFile, b, 0666)
	checkError(err)
}

func (r *runner) plan(filePath string, s os.FileInfo) {
	fs := uint64(s.Size())
	fmt.Println("Should backup", filePath, "with", humanize.Bytes(fs), "...")
}

func (r *runner) prune(filePath string) {
	fmt.Println("Pruning", filePath, "...")
	_, err := r.client.DeleteObject(r.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(r.host + filePath),
	})
	checkError(err)
	delete(r.metadata, filePath)
	r.saveMetadata()
}

func match(name string, regexps []*regexp.Regexp) bool {
	for _, re := range regexps {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
