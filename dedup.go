package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

/*
pyDedupe.go

A command line tool to identify duplicate files based on
size and md5 hash value.
*/

func main() {
	startTime := time.Now()

	var path string
	if len(os.Args) > 1 {
		path = os.Args[1]
	} else {
		path, _ = os.Getwd()
	}

	fileDict, duplicateList := scanFiles(path, 77)

	for dupe := range duplicateList {
		fmt.Printf("Duplicate files found for %s:\n", dupe)
		for _, file := range fileDict[dupe] {
			fmt.Printf("  %s\n", file["name"])
		}
	}

	fmt.Println("\nDone.")

	elapsedTime := time.Since(startTime)
	fmt.Printf("Total run time: %s\n", elapsedTime)
}

func scanFiles(path string, detail int) (map[string][]map[string]interface{}, map[string]bool) {
	duplicateList := make(map[string]bool)
	fileDict := make(map[string][]map[string]interface{})

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
		if detail > 0 && count%detail == 0 {
			log.Printf("Processed %d files...\r", count)
		}

		fileSize := info.Size()

		// Skip empty files
		if fileSize == 0 {
			return nil
		}

		startTime := time.Now()

		// Report large files
		if fileSize > 1024*1024*1024 {
			log.Printf("\nProcessing large (%dMB) file: %s\n", fileSize/(1024*1024), filePath)
		}

		fileHash, err := getMD5Hash(filePath)

		if err != nil {
			log.Printf("\nError processing file: %s\n", filePath)
			log.Printf("Exception: %s\n", err.Error())
			return nil
		}

		if fileSize > 1024*1024*1024 {
			elapsedTime := time.Since(startTime)
			log.Printf("File processed in %s\n\n", elapsedTime)
		}

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

	return fileDict, duplicateList
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
