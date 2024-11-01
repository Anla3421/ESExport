package importData

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"server/config"
)

func Import() {
	// 使用 Glob 來匹配所有 .json 檔案
	log.Println("import start")
	files, err := filepath.Glob(filepath.Join(config.Cfgs.ImportPath, "*.json"))
	if err != nil {
		log.Fatalf("glob fail: %v", err)
	}
	jsonMap := make(map[int]Hit)

	for i, file := range files {
		log.Printf("handling .json file No.%v...\n", i+1)
		// 讀取每個 .json 檔案
		data, err := os.ReadFile(file)
		if err != nil {
			log.Printf("readFile fail: %s: %v", file, err)
			continue
		}

		// 解析 JSON 資料到 Hit 結構
		var hit []Hit
		if err := json.Unmarshal(data, &hit); err != nil {
			log.Printf("Unmarshal fail: %s: %v", file, err)
			continue
		}

		// 將解析後的資料存入 map
		for k, v := range hit {
			jsonMap[k] = v
			// 每 x 筆先 espost，後清空 jsonMap 再繼續塞入
			if k >= config.Cfgs.ImportSize-1 {
				ExecImportData(jsonMap)
				jsonMap = map[int]Hit{}
			}
		}
	}
	ExecImportData(jsonMap)
	log.Println("import finish")
}

func ExecImportData(jsonMap map[int]Hit) {
	for _, v := range jsonMap {
		url := fmt.Sprintf("%s/%s/%s/%s", config.Cfgs.ImportESAddr, config.Cfgs.ImportIndex, "_doc", v.ID)
		ESPost(v.Source, url)
		// importRes := ESPost(v.Source, url)
		// fmt.Printf("No.%v , importRes: %+v\n", i, importRes)
	}
}

func ESPost(requestBody SourceData, url string) *ImportResponse {
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatalf("Error encoding JSON: %v", err)
	}

	// 忽略證書驗證
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error sending request: %v", err)
	}
	defer resp.Body.Close()

	// 拿到資料的處理
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}

	reutlt := &ImportResponse{}
	if err := json.Unmarshal(body, &reutlt); err != nil {
		log.Fatalf("Error decoding response JSON: %v", err)
	}
	return reutlt
}

// 實做尚未完成，body 會有沒有預期的錯誤產生
func ExecImportDataByBulk(jsonMap map[int]Hit) {
	// url := "http://10.85.1.220:30902/_bulk"
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
