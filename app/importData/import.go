package importData

import (
	"bytes"
	"encoding/json"
	"estool/config"
	"estool/delivery/esHttp"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func Import() {
	// 使用 Glob 來匹配所有 .json 檔案
	log.Println("import start")
	files, err := filepath.Glob(filepath.Join(config.Cfgs.ImportPath, "*.json"))
	if err != nil {
		log.Fatalf("glob fail: %v", err)
	}
	jsonMap := make(map[int]Hit)

	for _, file := range files {
		log.Printf("handling %v...\n", file)
		// 讀取每個 .json 檔案
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("readFile fail: %s: %v", file, err)
		}

		// 解析 JSON 資料到 Hit 結構
		var hit []Hit
		if err := json.Unmarshal(data, &hit); err != nil {
			log.Fatalf("Unmarshal fail: %s: %v", file, err)
		}

		// 將解析後的資料存入 map
		i := 0
		for k, v := range hit {
			jsonMap[k] = v
			// 每 x 筆或是檔案讀完了，先 espost，後清空 jsonMap 再繼續塞入
			if i >= config.Cfgs.ImportSize-1 || k == len(hit)-1 {
				ExecImportData(jsonMap)
				jsonMap = map[int]Hit{}
				i = 0
			}
			i++
		}
	}
	ExecImportData(jsonMap)
	log.Println("import finish")
}

func ExecImportData(jsonMap map[int]Hit) {
	for _, v := range jsonMap {
		jsonBody, err := json.Marshal(v.Source)
		if err != nil {
			log.Fatalf("Error encoding JSON: %v", err)
		}
		url := fmt.Sprintf("%s/%s/%s/%s", config.Cfgs.ImportESAddr, config.Cfgs.ImportIndex, "_doc", v.ID)
		esHttp.ESPost(jsonBody, url)

		// debug use
		// importRes := esHttp.ESPost(jsonBody, url)
		// fmt.Printf("importRes: %+v\n", importRes)

		time.Sleep(50 * time.Millisecond)
	}
}

// 實做尚未完成，body 會有沒有預期的錯誤產生
func ExecImportDataByBulk(jsonMap map[int]Hit) {
	importReq := ``
	for _, v := range jsonMap {
		indexData := ImportRequest{
			Index: Index{
				Index: v.Index,
				Type:  v.Type,
				ID:    v.ID,
			},
		}

		indexJSON, err := json.Marshal(indexData)
		if err != nil {
			log.Fatal("indexJSON marshal fail")
		}
		dataJSON, err := json.Marshal(v.Source)
		if err != nil {
			log.Fatal("dataJSON marshal fail")
		}
		importReq += fmt.Sprintf(`%s
	%s
	`, indexJSON, dataJSON)
	}

	url := fmt.Sprintf("%s/_bulk", config.Cfgs.ImportESAddr)
	fmt.Println("url", url)
	fmt.Printf("%+v\n", importReq)

	// importRes := ESPost(importReq, url)
	// fmt.Printf("No. , importRes: %+v\n", importRes)
	// ESPost(importReq, url)
}

// ImportByOpenSearchBulk 使用 OpenSearch bulk API 進行匯入
func ImportByOpenSearchBulk() {
	log.Println("import start with OpenSearch bulk API")

	checkpointFile := getCheckpointFilePath()
	processedFiles := loadProcessedFiles(checkpointFile)
	jsonFiles := getJsonFiles()

	checkpointWriter := createCheckpointWriter(checkpointFile)
	defer checkpointWriter.Close()

	importFiles(jsonFiles, processedFiles, checkpointWriter)
	log.Println("import finish")
}

// getCheckpointFilePath 獲取檢查點文件路徑
func getCheckpointFilePath() string {
	return filepath.Join(config.Cfgs.ImportPath, "import_checkpoint.txt")
}

// loadProcessedFiles 加載已處理的文件記錄
func loadProcessedFiles(checkpointFile string) map[string]bool {
	processedFiles := make(map[string]bool)
	data, err := os.ReadFile(checkpointFile)
	if err == nil {
		files := strings.Split(string(data), "\n")
		for _, file := range files {
			if file != "" {
				processedFiles[file] = true
			}
		}
		log.Printf("Found checkpoint with %d processed files", len(processedFiles))
	}
	return processedFiles
}

// getJsonFiles 獲取所有JSON文件
func getJsonFiles() []string {
	files, err := filepath.Glob(filepath.Join(config.Cfgs.ImportPath, "*.json"))
	if err != nil {
		log.Fatalf("glob fail: %v", err)
	}
	return files
}

// createCheckpointWriter 創建檢查點文件寫入器
func createCheckpointWriter(checkpointFile string) *os.File {
	writer, err := os.OpenFile(checkpointFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Warning: Could not open checkpoint file: %v", err)
	}
	return writer
}

// importFiles 處理所有文件的導入
func importFiles(files []string, processedFiles map[string]bool, checkpointWriter *os.File) {
	var bulkBody bytes.Buffer
	count := 0

	for _, file := range files {
		if processedFiles[filepath.Base(file)] {
			log.Printf("Skipping already processed file: %s", filepath.Base(file))
			continue
		}

		if processFile(file, &bulkBody, &count, checkpointWriter) {
			recordProcessedFile(checkpointWriter, file)
		}
	}

	// 處理剩餘的文檔
	if bulkBody.Len() > 0 {
		if err := executeBulkRequest(&bulkBody); err != nil {
			log.Printf("Error executing final bulk request: %v", err)
		}
	}
}

// processFile 處理單個文件
func processFile(file string, bulkBody *bytes.Buffer, count *int, checkpointWriter *os.File) bool {
	log.Printf("handling %v...\n", file)
	hits, err := readJsonFile(file)
	if err != nil {
		return false
	}

	return processHits(hits, bulkBody, count)
}

// readJsonFile 讀取並解析JSON文件
func readJsonFile(file string) ([]Hit, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		log.Printf("Error reading file %s: %v", file, err)
		return nil, err
	}

	var hits []Hit
	if err := json.Unmarshal(data, &hits); err != nil {
		log.Printf("Error unmarshaling file %s: %v", file, err)
		return nil, err
	}
	return hits, nil
}

// processHits 處理所有hits數據
func processHits(hits []Hit, bulkBody *bytes.Buffer, count *int) bool {
	for _, hit := range hits {
		if err := processBulkRequest(bulkBody, hit, count); err != nil {
			log.Printf("Error processing bulk request: %v", err)
			return false
		}
	}
	return true
}

// recordProcessedFile 記錄已處理的文件
func recordProcessedFile(writer *os.File, file string) {
	if _, err := writer.WriteString(filepath.Base(file) + "\n"); err != nil {
		log.Printf("Warning: Could not write to checkpoint file: %v", err)
	}
	writer.Sync()
	log.Printf("Successfully processed file: %s", filepath.Base(file))
}

// processBulkRequest 處理單個文檔的 bulk request
func processBulkRequest(bulkBody *bytes.Buffer, hit Hit, count *int) error {
	// 建立 action metadata
	action := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, config.Cfgs.ImportIndex, hit.ID)
	bulkBody.WriteString(action + "\n")

	// 建立 document body
	docJSON, err := json.Marshal(hit.Source)
	if err != nil {
		return fmt.Errorf("error marshaling document: %v", err)
	}
	bulkBody.Write(docJSON)
	bulkBody.WriteString("\n")

	*count++

	// 當達到批次大小時執行 bulk request
	if *count >= config.Cfgs.ImportSize {
		if err := executeBulkRequest(bulkBody); err != nil {
			return err
		}
		bulkBody.Reset()
		*count = 0
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

// executeBulkRequest 執行 bulk request
func executeBulkRequest(bulkBody *bytes.Buffer) error {
	url := fmt.Sprintf("%s/_bulk?refresh=true", config.Cfgs.ImportESAddr)

	// 使用已有的 ESPost 函數
	resp := esHttp.ESPost(bulkBody.Bytes(), url)

	// 解析回應
	var bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				Status int `json:"status"`
				Error  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}

	if err := json.Unmarshal(resp, &bulkResponse); err != nil {
		log.Printf("Error parsing bulk response: %v", err)
		return err
	}

	// 檢查錯誤
	if bulkResponse.Errors {
		for _, item := range bulkResponse.Items {
			if item.Index.Status >= 400 {
				log.Printf("Error indexing document: status=%d, type=%s, reason=%s",
					item.Index.Status,
					item.Index.Error.Type,
					item.Index.Error.Reason)
			}
		}
	}

	return nil
}
