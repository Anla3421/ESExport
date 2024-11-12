package importData

import (
	"encoding/json"
	"estool/config"
	"estool/delivery/esHttp"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		// fmt.Printf("No.%v , importRes: %+v\n", i, importRes)
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
