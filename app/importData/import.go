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
	files, err := filepath.Glob(filepath.Join(config.Cfgs.ImportPath, "*.json"))
	if err != nil {
		log.Fatalf("glob fail: %v", err)
	}

	// 準備 bulk request
	var bulkBody bytes.Buffer
	count := 0

	for _, file := range files {
		log.Printf("handling %v...\n", file)
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("readFile fail: %s: %v", file, err)
		}

		var hits []Hit
		if err := json.Unmarshal(data, &hits); err != nil {
			log.Fatalf("Unmarshal fail: %s: %v", file, err)
		}

		for _, hit := range hits {
			// 建立 action metadata
			action := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, config.Cfgs.ImportIndex, hit.ID)
			bulkBody.WriteString(action + "\n")

			// 建立 document body
			docJSON, err := json.Marshal(hit.Source)
			if err != nil {
				log.Printf("Error marshaling document: %v", err)
				continue
			}
			bulkBody.Write(docJSON)
			bulkBody.WriteString("\n")

			count++

			// 當達到批次大小時執行 bulk request
			if count >= config.Cfgs.ImportSize {
				executeBulkRequest(&bulkBody)
				bulkBody.Reset()
				count = 0
				time.Sleep(100 * time.Millisecond) // 避免過度頻繁請求
			}
		}
	}

	// 處理剩餘的文檔
	if bulkBody.Len() > 0 {
		executeBulkRequest(&bulkBody)
	}

	log.Println("import finish")
}

// executeBulkRequest 執行 bulk request
func executeBulkRequest(bulkBody *bytes.Buffer) {
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
		return
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
}
