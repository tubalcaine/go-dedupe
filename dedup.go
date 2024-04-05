package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

/*
dedup.go

A command line tool to identify duplicate files based on
size and md5 hash value.
*/

type ScanOptions struct {
	MaxMB  int
	Detail int
}

func scanFiles(path string, options ScanOptions) (map[string][]map[string]interface{}, map[string]bool, []string, []string) {
	duplicateList := make(map[string]bool)
	fileDict := make(map[string][]map[string]interface{})
	zeroLengthFiles := make([]string, 0)
	largeFiles := make([]string, 0)

	count := 0

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing file: %s\n", filePath)
			log.Printf("Error: %s\n", err.Error())
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		count++
		if options.Detail > 0 && count%options.Detail == 0 {
			log.Printf("Processed %d files...\r", count)
		}

		fileSize := info.Size()

		// Skip empty files
		if fileSize == 0 {
			zeroLengthFiles = append(zeroLengthFiles, filePath)
			return nil
		}

		// Skip large files
		if int64(options.MaxMB) > 0 && fileSize > int64(options.MaxMB)*1024*1024 {
			log.Printf("Skipping VERY large %.2fMB file: %s\n", float64(options.MaxMB)/(1024*1024), filePath)
			largeFiles = append(largeFiles, filePath)
			return nil
		}

		// Warn of files larger than 4GB
		if fileSize > 4*1024*1024*1024 {
			fmt.Fprintf(os.Stderr, "Processing large (%.2f MB) file: %s\n", float64(fileSize)/(1024*1024), filePath)
		}

		startTime := time.Now()

		fileHash, err := getMD5Hash(filePath)

		if err != nil {
			log.Printf("\nError processing file: %s\n", filePath)
			log.Printf("Exception: %s\n", err.Error())
			return nil
		}

		elapsedTime := time.Since(startTime)
		log.Printf("File processed in %s\n\n", elapsedTime)

		key := fmt.Sprintf("%d:%s", fileSize, fileHash)
		fileInfo := map[string]interface{}{
			"name":     filePath,
			"size":     fileSize,
			"md5_hash": fileHash,
			"key":      key,
		}

		if _, ok := fileDict[key]; ok {
			fileDict[key] = append(fileDict[key], fileInfo)
			duplicateList[key] = true
		} else {
			fileDict[key] = []map[string]interface{}{fileInfo}
		}

		return nil
	})

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

func main() {
	startTime := time.Now()

	// Define command line flags
	detail := flag.Int("detail", 77, "Set how often to print a status message (default 77 files)")
	maxMB := flag.Int("maxmb", 0, "Set the maximum file size in megabytes (default 0 for no limit)")

	flag.Parse()

	var path string
	if len(os.Args) > 1 {
		path = os.Args[1]
	} else {
		path, _ = os.Getwd()
	}

	options := ScanOptions{
		MaxMB:  *maxMB,
		Detail: *detail,
	}

	fileDict, duplicateList, zeroLengthFiles, oversizeFiles := scanFiles(path, options)

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
