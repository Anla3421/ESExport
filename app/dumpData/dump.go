package dumpData

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"server/config"
	"strings"
	"time"
)

var (
	importData []Hit
	i          int = 1
)

// 戳 es 拿資料
func Dump() {
	log.Println("dump start")
	startDate := HandleIndexString(config.Cfgs.DumpIndexStart)
	endDate := HandleIndexString(config.Cfgs.DumpIndexEnd)

	option := "_search?ignore_unavailable=true&allow_no_indices=true&preference=_primary&"
	scrollTime := "scroll=5m"
	requestBody := map[string]interface{}{
		"size": config.Cfgs.DumpPostSize,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"startTime": map[string]string{
								"gte": config.Cfgs.DumpGte,
								"lte": config.Cfgs.DumpLte,
							},
						},
					},
				},
			},
		},
	}

	for date := startDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
		indexToDump := fmt.Sprintf("logs-%s", date.Format("2006.01.02"))
		url := fmt.Sprintf("%s/%s/%s%s", config.Cfgs.DumpESAddr, indexToDump, option, scrollTime)
		scrollRes := ESPost(requestBody, url)
		HandleRemainData(scrollRes, indexToDump)
	}

	// 撈取 by 數量
	// url := fmt.Sprintf("%s/%s/%s%s", config.Cfgs.DumpESAddr, "logs-*", option, scrollTime)
	// fmt.Println("url", url)
	// scrollRes := ESPost(requestBody, url)
	// HandleRemainDataByAmount(scrollRes)
	// log.Println("dump finish")
}

// 匯入 by index
func HandleRemainData(scrollRes *ScrollResponse, index string) {
	for k := range scrollRes.Hits.Hits {
		resData := Hit{
			Index:  scrollRes.Hits.Hits[k].Index,
			Type:   scrollRes.Hits.Hits[k].Type,
			ID:     scrollRes.Hits.Hits[k].ID,
			Score:  scrollRes.Hits.Hits[k].Score,
			Source: scrollRes.Hits.Hits[k].Source,
		}
		importData = append(importData, resData)
	}

	// scroll 完該 index 的資料後塞入 .json 檔，如果沒有資料則不產生檔案
	if (len(scrollRes.Hits.Hits) == 0) && (len(importData) != 0) {
		// json beauty
		jsonData, err := json.MarshalIndent(importData, "", "    ")
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}
		fileName := fmt.Sprintf("%s/%s.json", config.Cfgs.DumpPath, index)
		err = os.WriteFile(fileName, jsonData, 0644)
		if err != nil {
			log.Fatalf("Error writing to file %s: %v", fileName, err)
		}
		log.Printf("Data written to %s successfully.", fileName)
		importData = []Hit{}
	}

	// 還有資料就繼續遞迴
	if len(scrollRes.Hits.Hits) > 0 {
		// 再次去戳 es 撈資料
		url := fmt.Sprintf("%s/_search/scroll", config.Cfgs.DumpESAddr)
		requestBody := map[string]interface{}{
			"scroll":    "5m",
			"scroll_id": scrollRes.ScrollID,
		}
		scrollRes = ESPost(requestBody, url)
		// time.Sleep(50 * time.Millisecond) // 測試用煞車，可以拔掉
		HandleRemainData(scrollRes, index)
	}
}

// 匯入 by 筆數
func HandleRemainDataByAmount(scrollRes *ScrollResponse) {
	for k := range scrollRes.Hits.Hits {
		resData := Hit{
			Index:  scrollRes.Hits.Hits[k].Index,
			Type:   scrollRes.Hits.Hits[k].Type,
			ID:     scrollRes.Hits.Hits[k].ID,
			Score:  scrollRes.Hits.Hits[k].Score,
			Source: scrollRes.Hits.Hits[k].Source,
		}
		importData = append(importData, resData)
	}

	// 滿 config.Cfgs.DumpLenImportData 筆或撈不到東西就塞現有資料進去.json檔
	if len(importData) >= config.Cfgs.DumpLenImportData || len(scrollRes.Hits.Hits) == 0 {
		// json beauty
		jsonData, err := json.MarshalIndent(importData, "", "    ")
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}

		fileName := fmt.Sprintf("%s/%d.json", config.Cfgs.DumpPath, i)
		log.Println("fileName check...: ", fileName)

		err = os.WriteFile(fileName, jsonData, 0644)
		if err != nil {
			log.Fatalf("Error writing to file: %v", err)
		}
		log.Println("Data written to .json successfully.")
		importData = []Hit{}
		i++
	}

	// 還有資料就繼續遞迴
	if len(scrollRes.Hits.Hits) > 0 {
		// 再去戳 es 撈資料
		url := "http://10.85.1.218:30902/_search/scroll"
		requestBody := map[string]interface{}{
			"scroll":    "5m",
			"scroll_id": scrollRes.ScrollID,
		}
		scrollRes = ESPost(requestBody, url)
		// time.Sleep(50 * time.Millisecond) // 測試用煞車，可以拔掉
		HandleRemainDataByAmount(scrollRes)
	}
}

func HandleIndexString(IndexString string) time.Time {
	temp := strings.Split(IndexString, "-")
	if len(temp) != 2 {
		log.Fatalf("IndexString split fail")
		return time.Time{}
	}
	// 提取日期部分並將其轉換為 time.Time
	dateString := temp[1]
	dateTime, err := time.Parse("2006.01.02", dateString) // 使用適當的日期格式
	if err != nil {
		log.Fatalf("date parse fail: %v\n", err)
		return time.Time{}
	}
	return dateTime
}

func ESPost(requestBody map[string]interface{}, url string) *ScrollResponse {
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatalf("Error encoding JSON: %v", err)
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

	reutlt := &ScrollResponse{}
	if err := json.Unmarshal(body, &reutlt); err != nil {
		log.Fatalf("Error decoding response JSON: %v", err)
	}
	return reutlt
}
