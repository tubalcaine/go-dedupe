package main

// Package main implements a command line tool to identify duplicate files based on
// size and MD5 hash value. It provides options to limit the maximum file size for
// MD5 calculation and the frequency of status messages.
//
// ScanOptions defines the maximum file size to calculate the MD5 hash, the frequency
// to print a status message, and the maximum length of the MD5 calculation queue.
//
// scanFiles scans the specified directory for files, calculates their MD5 hash, and
// identifies duplicate files. It returns a dictionary of files, a list of duplicates,
// zero-length files, and files that exceed the maximum size.
//
// getMD5Hash calculates the MD5 hash of a given file and returns it as a string.
//
// The main function parses command line flags, sets up scan options, and initiates
// the file scanning process. It prints the results, including duplicate files,
// zero-length files, and oversized files, and displays the total run time.

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type ScanOptions struct {
	MaxMB          int
	Detail         int
	MaxQueueLength int
	RegExes        []*regexp.Regexp
}

func scanFiles(path string, options ScanOptions, totalCount int) (map[string][]map[string]interface{}, map[string]bool, []string, []string) {
	duplicateList := make(map[string]bool)
	fileDict := make(map[string][]map[string]interface{})
	zeroLengthFiles := make([]string, 0)
	largeFiles := make([]string, 0)

	count := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	md5Queue := make(chan struct{}, options.MaxQueueLength)

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing file: %s\n", filePath)
			log.Printf("Error: %s\n", err.Error())
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		if len(options.RegExes) > 0 {
			matched := false
			for _, re := range options.RegExes {
				if re.MatchString(filepath.Base(filePath)) {
					matched = true
					break
				}
			}
			if !matched {
				return nil
			}
		}

		count++
		if options.Detail > 0 && count%options.Detail == 0 {
			if totalCount > 0 {
				percentComplete := float64(count) / float64(totalCount) * 100
				log.Printf("Processed %d of %d files (%.2f%%).\t%s\r", count, totalCount, percentComplete, filepath.Dir(filePath))
			} else {
				log.Printf("Processed %d files.\t%s\r", count, filepath.Dir(filePath))
			}
		}

		fileSize := info.Size()

		// Skip empty files
		if fileSize == 0 {
			zeroLengthFiles = append(zeroLengthFiles, filePath)
			return nil
		}

		// Skip large files
		if int64(options.MaxMB) > 0 && fileSize > int64(options.MaxMB)*1024*1024 {
			log.Printf("Skipping VERY large %.2fMB file: %s\n", float64(fileSize)/(1024*1024), filePath)
			largeFiles = append(largeFiles, filePath)
			return nil
		}

		// Warn of files larger than 4GB
		if fileSize > 4*1024*1024*1024 {
			fmt.Fprintf(os.Stderr, "Processing large (%.2f MB) file: %s\n", float64(fileSize)/(1024*1024), filePath)
		}

		wg.Add(1)
		md5Queue <- struct{}{} // Add to the queue

		go func(filePath string, fileSize int64) {
			defer wg.Done()
			defer func() { <-md5Queue }() // Remove from the queue

			startTime := time.Now()

			fileHash, err := getMD5Hash(filePath)
			if err != nil {
				log.Printf("\nError processing file: %s\n", filePath)
				log.Printf("Exception: %s\n", err.Error())
				//
				fileHash = "00000000000000000000000000000000"
			}

			elapsedTime := time.Since(startTime)

			if fileSize > 4*1024*1024*1024 {
				log.Printf("File processed in %s\n\n", elapsedTime)
			}

			key := fmt.Sprintf("%d:%s", fileSize, fileHash)
			fileInfo := map[string]interface{}{
				"name":     filePath,
				"size":     fileSize,
				"md5_hash": fileHash,
				"key":      key,
			}

			mu.Lock()
			defer mu.Unlock()

			if fileHash != "00000000000000000000000000000000" {
				if _, ok := fileDict[key]; ok {
					fileDict[key] = append(fileDict[key], fileInfo)
					duplicateList[key] = true
				} else {
					fileDict[key] = []map[string]interface{}{fileInfo}
				}
			}
		}(filePath, fileSize)

		return nil
	})

	wg.Wait() // Wait for all goroutines to finish

	if err != nil {
		log.Printf("Error scanning files: %s\n", err.Error())
	}

	return fileDict, duplicateList, zeroLengthFiles, largeFiles
}

func getMD5Hash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func countFiles(path string, options ScanOptions) (int, error) {
	count := 0
	detail := options.Detail
	regexpList := options.RegExes
	totalCount := 0

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		// Make errors non-fatal in counting mode
		if err != nil {
			log.Printf("Error accessing file: %s\nError: %s\n", filePath, err.Error())
			//			return err
			return nil
		}

		if info.Mode().IsRegular() {
			totalCount++
			if len(regexpList) > 0 {
				for _, re := range regexpList {
					if re.MatchString(filepath.Base(filePath)) {
						count++
					}

					if detail > 0 && count%detail == 0 {
						log.Printf("Counted %d files of which %d matched a regex.\n Currently in dir %s.\n",
							totalCount, count, filepath.Dir(filePath))
					}
				}
			} else {
				count++
				if detail > 0 && count%detail == 0 {
					log.Printf("Counted %d files in %s.\n", count, filepath.Dir(filePath))
				}
			}
		}
		return nil
	})
	return count, err
}

func main() {
	startTime := time.Now()

	// Define command line flags
	detail := flag.Int("detail", 77, "Set how often to print a status message (default 77 files)")
	maxMB := flag.Int("maxmb", 0, "Set the maximum file size in megabytes (default 0 for no limit)")
	maxQueueLength := flag.Int("maxQueueLength", 5, "Set the maximum number of concurrent MD5 calculations")
	path := flag.String("path", ".", "Set the path to scan (default current directory)")
	precount := flag.Bool("precount", false, "Pre-count the total number of files before scanning")

	var regexList []string

	flag.Func("regex", "Set a regular expression to filter files (can be used multiple times)", func(s string) error {
		regexList = append(regexList, s)
		return nil
	})

	flag.Parse()

	var compiledRegexes []*regexp.Regexp
	for _, r := range regexList {
		compiledRegex, err := regexp.Compile(r)
		if err != nil {
			log.Fatalf("Error compiling regex: %s\n", err.Error())
		}
		compiledRegexes = append(compiledRegexes, compiledRegex)
	}

	var totalCount int

	options := ScanOptions{
		MaxMB:          *maxMB,
		Detail:         *detail,
		MaxQueueLength: *maxQueueLength,
		RegExes:        compiledRegexes,
	}

	if *precount {
		var err error
		totalCount, err = countFiles(*path, options)
		if err != nil {
			log.Fatalf("Error counting files: %s\n", err.Error())
		}
		fmt.Printf("Total number of files to scan: %d\n", totalCount)
	}

	fileDict, duplicateList, zeroLengthFiles, oversizeFiles := scanFiles(*path, options, totalCount)

	for dupe := range duplicateList {
		fmt.Printf("Duplicate files found for %s:\n", dupe)
		for _, file := range fileDict[dupe] {
			fmt.Printf("  %s\n", file["name"])
		}
	}

	fmt.Println("\nZero length files:")

	for _, file := range zeroLengthFiles {
		fmt.Printf("  %s\n", file)
	}

	fmt.Println("\nOversize files:")

	for _, file := range oversizeFiles {
		fmt.Printf("  %s\n", file)
	}

	fmt.Println("\nDone.")

	elapsedTime := time.Since(startTime)
	fmt.Printf("Total run time: %s\n", elapsedTime)
}
