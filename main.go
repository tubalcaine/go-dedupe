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
	"encoding/json"
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

// matchesRegexFilters checks if a file matches any of the provided regex filters
// If no filters are provided, it returns true
func matchesRegexFilters(filename string, regexes []*regexp.Regexp) bool {
	if len(regexes) == 0 {
		return true
	}
	
	for _, re := range regexes {
		if re.MatchString(filepath.Base(filename)) {
			return true
		}
	}
	return false
}

// getUniqueFilePath returns a unique file path by adding a numeric suffix if needed
func getUniqueFilePath(basePath string) string {
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		// Path doesn't exist, it's already unique
		return basePath
	}
	
	// Path exists, need to create a unique name
	ext := filepath.Ext(basePath)
	nameWithoutExt := basePath[:len(basePath)-len(ext)]
	
	// Try adding numeric suffixes until we find a unique name
	counter := 1
	for {
		newPath := fmt.Sprintf("%s_%d%s", nameWithoutExt, counter, ext)
		if _, err := os.Stat(newPath); os.IsNotExist(err) {
			return newPath
		}
		counter++
	}
}

// copyFile copies a file from src to dst and properly cleans up resources
func copyFile(src, dst string) error {
	// Open the source file
	input, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("error opening source file %s: %w", src, err)
	}
	defer input.Close() // This will close when the function returns
	
	// Create the destination file
	output, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("error creating destination file %s: %w", dst, err)
	}
	defer output.Close() // This will close when the function returns
	
	// Create buffered readers/writers for improved performance
	const bufferSize = 4 * 1024 * 1024 // 4MB buffer
	buf := make([]byte, bufferSize)
	
	// Copy the file contents in chunks
	for {
		n, err := input.Read(buf)
		if n > 0 {
			if _, err := output.Write(buf[:n]); err != nil {
				return fmt.Errorf("error writing to destination file %s: %w", dst, err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from source file %s: %w", src, err)
		}
	}
	
	return nil
}

// writeDuplicateList writes a list of duplicate files to the specified path, excluding the original file
func writeDuplicateList(files []map[string]interface{}, originalFile, listPath string) error {
	// Handle path collisions by creating a unique file path
	uniqueListPath := getUniqueFilePath(listPath)
	
	// Create the duplicate list file
	dupListFile, err := os.Create(uniqueListPath)
	if err != nil {
		return fmt.Errorf("error creating duplicate list file %s: %w", uniqueListPath, err)
	}
	defer dupListFile.Close() // This will close when the function returns
	
	// Write all duplicate files except the original to the list
	for _, file := range files {
		if file["name"].(string) != originalFile {
			fmt.Fprintf(dupListFile, "%s\n", file["name"].(string))
		}
	}
	
	// Log if we had to use a different path
	if uniqueListPath != listPath {
		log.Printf("Duplicate list file already exists, created %s instead\n", uniqueListPath)
	}
	
	return nil
}

func scanFiles(path string, options ScanOptions, totalCount int) (map[string][]map[string]interface{}, map[string]bool, []string, []string) {
	duplicateList := make(map[string]bool)
	fileDict := make(map[string][]map[string]interface{})
	zeroLengthFiles := make([]string, 0, 100)  // Pre-allocate capacity
	largeFiles := make([]string, 0, 100)       // Pre-allocate capacity

	count := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	md5Queue := make(chan struct{}, options.MaxQueueLength)

	// Create a buffer pool to reduce GC pressure
	bufferPool := sync.Pool{
		New: func() interface{} {
			// 4MB buffer for file reads
			return make([]byte, 4*1024*1024)
		},
	}

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing file: %s\n", filePath)
			log.Printf("Error: %s\n", err.Error())
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		if !matchesRegexFilters(filePath, options.RegExes) {
			return nil
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
			mu.Lock()
			zeroLengthFiles = append(zeroLengthFiles, filePath)
			mu.Unlock()
			return nil
		}

		// Skip large files
		if int64(options.MaxMB) > 0 && fileSize > int64(options.MaxMB)*1024*1024 {
			log.Printf("Skipping VERY large %.2fMB file: %s\n", float64(fileSize)/(1024*1024), filePath)
			mu.Lock()
			largeFiles = append(largeFiles, filePath)
			mu.Unlock()
			return nil
		}

		// Warn of files larger than 4GB
		if fileSize > 4*1024*1024*1024 {
			fmt.Fprintf(os.Stderr, "Processing large (%.2f MB) file: %s\n", float64(fileSize)/(1024*1024), filePath)
		}

		wg.Add(1)
		md5Queue <- struct{}{} // Add to the queue

		go func(filePath string, fileSize int64, modTime time.Time) {
			defer wg.Done()
			defer func() { <-md5Queue }() // Remove from the queue

			startTime := time.Now()

			// Use custom MD5 calculation with buffer from pool
			file, err := os.Open(filePath)
			var fileHash string
			
			if err != nil {
				log.Printf("\nError opening file: %s\n", filePath)
				log.Printf("Exception: %s\n", err.Error())
				fileHash = "00000000000000000000000000000000"
			} else {
				defer file.Close()
				
				// Get a buffer from the pool
				buf := bufferPool.Get().([]byte)
				defer bufferPool.Put(buf) // Return the buffer to the pool
				
				hash := md5.New()
				
				// Read file in chunks
				for {
					n, err := file.Read(buf)
					if n > 0 {
						hash.Write(buf[:n])
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Printf("\nError reading file: %s\n", filePath)
						log.Printf("Exception: %s\n", err.Error())
						fileHash = "00000000000000000000000000000000"
						break
					}
				}
				
				if fileHash == "" {
					fileHash = fmt.Sprintf("%x", hash.Sum(nil))
				}
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
				"mtime":    modTime,
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
		}(filePath, fileSize, info.ModTime())

		return nil
	})

	wg.Wait() // Wait for all goroutines to finish

	if err != nil {
		log.Printf("Error scanning files: %s\n", err.Error())
	}

	return fileDict, duplicateList, zeroLengthFiles, largeFiles
}

func countFiles(path string, options ScanOptions) (int, error) {
	count := 0
	detail := options.Detail
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

			if detail > 0 && totalCount%detail == 0 {
				log.Printf("Counted %d files of which %d matched a regex.\n Currently in dir %s.\n",
					totalCount, count, filepath.Dir(filePath))
			}

			if matchesRegexFilters(filePath, options.RegExes) {
				count++
			}
		}
		return nil
	})
	return count, err
}

func main() {
	startTime := time.Now()

	// Define command line flags
	detail := flag.Int("detail", 77, "Set how often to print a status message")
	maxMB := flag.Int("maxmb", 0, "Set the maximum file size in megabytes (default 0 for no limit)")
	maxQueueLength := flag.Int("maxQueueLength", 5, "Set the maximum number of concurrent MD5 calculations")
	path := flag.String("path", ".", "Set the path to scan")
	precount := flag.Bool("precount", false, "Pre-count the total number of files before scanning")
	jsonOutput := flag.String("json", "", "Set the file path to save the scan results in JSON format")
	uniqFilesPath := flag.String("uniqFilesPath", "", "Set the dir/folder to save one unique file of each set of duplicates")

	var regexList []string

	flag.Func("regex", "Set a regular expression to filter files (can be used multiple times)", func(s string) error {
		regexList = append(regexList, s)
		return nil
	})

	flag.Parse()
	if *uniqFilesPath != "" {
		err := os.MkdirAll(*uniqFilesPath, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating unique files path: %s\n", err.Error())
		}
	}

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
	if *jsonOutput != "" {
		output := map[string]interface{}{
			"fileDict":        fileDict,
			"duplicateList":   duplicateList,
			"zeroLengthFiles": zeroLengthFiles,
			"oversizeFiles":   oversizeFiles,
			"maxMB":           *maxMB,
		}

		file, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error creating JSON output file: %s\n", err.Error())
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			log.Fatalf("Error encoding JSON output: %s\n", err.Error())
		}
	}

	for dupe := range duplicateList {
		fmt.Printf("Duplicate files found for %s:\n", dupe)
		dupeFiles := fileDict[dupe]
		if len(dupeFiles) > 0 {
			// Find the file with the latest modification time
			var latestFile map[string]interface{}
			for _, file := range dupeFiles {
				if latestFile == nil || file["mtime"].(time.Time).After(latestFile["mtime"].(time.Time)) {
					latestFile = file
				}
			}
			firstFile := latestFile["name"].(string)
			baseDstPath := filepath.Join(*uniqFilesPath, filepath.Base(firstFile))
			
			// Handle file name collisions
			uniqFilePath := getUniqueFilePath(baseDstPath)
			
			// Log if we had to use a different path than expected
			if uniqFilePath != baseDstPath {
				log.Printf("File %s already exists, using %s instead\n", 
					filepath.Base(baseDstPath), filepath.Base(uniqFilePath))
			}

			// Copy the file with the latest modification time to the unique file path
			if err := copyFile(firstFile, uniqFilePath); err != nil {
				log.Printf("%s\n", err)
				continue
			}

			// Create the duplicate list file
			dupListFilePath := uniqFilePath + "-dup-list.txt"
			if err := writeDuplicateList(dupeFiles, firstFile, dupListFilePath); err != nil {
				log.Printf("%s\n", err)
			}
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