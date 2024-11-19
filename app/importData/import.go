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

	// 檢查斷點續傳記錄檔
	checkpointFile := filepath.Join(config.Cfgs.ImportPath, "import_checkpoint.txt")
	fmt.Println("checkpointFile", checkpointFile)
	var processedFiles map[string]bool = make(map[string]bool)

	// 讀取已處理檔案的記錄
	if data, err := os.ReadFile(checkpointFile); err == nil {
		fmt.Println("data", data)
		files := strings.Split(string(data), "\n")
		for _, file := range files {
			if file != "" {
				processedFiles[file] = true
			}
		}
		log.Printf("Found checkpoint with %d processed files", len(processedFiles))
	}

	files, err := filepath.Glob(filepath.Join(config.Cfgs.ImportPath, "*.json"))
	if err != nil {
		log.Fatalf("glob fail: %v", err)
	}

	// 準備 bulk request
	var bulkBody bytes.Buffer
	count := 0

	// 開啟檔案用於追加記錄已處理的檔案，若檔案不存在則會創建它
	checkpointWriter, err := os.OpenFile(checkpointFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Warning: Could not open checkpoint file: %v", err)
	}
	defer checkpointWriter.Close()

	for _, file := range files {
		// 跳過已處理的檔案
		if processedFiles[filepath.Base(file)] {
			log.Printf("Skipping already processed file: %s", filepath.Base(file))
			continue
		}

		log.Printf("handling %v...\n", file)
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("Error reading file %s: %v, skipping...", file, err)
			continue
		}

		var hits []Hit
		if err := json.Unmarshal(data, &hits); err != nil {
			log.Fatalf("Error unmarshaling file %s: %v, skipping...", file, err)
			continue
		}

		// 處理檔案內容...
		importSuccess := true
		for _, hit := range hits {
			if err := processBulkRequest(&bulkBody, hit, &count); err != nil {
				log.Printf("Error processing bulk request: %v", err)
				importSuccess = false
				break
			}
		}

		// 如果檔案處理成功，記錄到 checkpoint 文件
		if importSuccess {
			if _, err := checkpointWriter.WriteString(filepath.Base(file) + "\n"); err != nil {
				log.Printf("Warning: Could not write to checkpoint file: %v", err)
			}
			checkpointWriter.Sync()
			log.Printf("Successfully processed file: %s", filepath.Base(file))
		}
	}

	// 處理剩餘的文檔
	if bulkBody.Len() > 0 {
		if err := executeBulkRequest(&bulkBody); err != nil {
			log.Printf("Error executing final bulk request: %v", err)
		}
	}

	log.Println("import finish")
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
