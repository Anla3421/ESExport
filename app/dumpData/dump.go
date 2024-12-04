package dumpData

import (
	"encoding/json"
	"estool/config"
	"estool/delivery/esHttp"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
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
		indexToDump := fmt.Sprintf(LogsIndexFormat, date.Format(DateFormat))
		url := fmt.Sprintf("%s/%s/%s%s", config.Cfgs.DumpESAddr, indexToDump, option, scrollTime)
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf(ErrEncodingJSON, err)
		}
		reutlt := esHttp.ESPost(jsonBody, url)
		scrollRes := &ScrollResponse{}
		if err := json.Unmarshal(reutlt, &scrollRes); err != nil {
			log.Fatalf(ErrDecodingResponseJSON, err)
		}
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
			log.Fatalf(ErrEncodingJSON, err)
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
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf(ErrEncodingJSON, err)
		}
		reutlt := esHttp.ESPost(jsonBody, url)
		scrollRes = &ScrollResponse{}
		if err := json.Unmarshal(reutlt, &scrollRes); err != nil {
			log.Fatalf(ErrDecodingResponseJSON, err)
		}
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
			log.Fatalf(ErrEncodingJSON, err)
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
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf(ErrEncodingJSON, err)
		}
		reutlt := esHttp.ESPost(jsonBody, url)
		scrollRes = &ScrollResponse{}
		if err := json.Unmarshal(reutlt, &scrollRes); err != nil {
			log.Fatalf(ErrDecodingResponseJSON, err)
		}
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
	dateTime, err := time.Parse(DateFormat, dateString)
	if err != nil {
		log.Fatalf("date parse fail: %v\n", err)
		return time.Time{}
	}
	return dateTime
}

// DumpWithBatch 每當資料達到設定時，記錄輸出一個批次檔案
func DumpWithBatch() {
	log.Println("dump with batch start")

	// 斷點續執行功能實做，檢查目錄下的檔案
	files, err := os.ReadDir(config.Cfgs.DumpPath)
	if err != nil {
		if os.IsNotExist(err) {
			// 目錄不存在，建立目錄
			if err := os.MkdirAll(config.Cfgs.DumpPath, 0755); err != nil {
				log.Fatalf("Failed to create directory: %v", err)
			}
		} else {
			log.Fatalf("Failed to read directory: %v", err)
		}
	}

	var latestDate time.Time
	if len(files) > 0 {
		// 遍歷所有檔案找出最新日期
		for _, file := range files {
			if !file.IsDir() {
				fileName := file.Name()
				// 使用正則表達式匹配檔案名中的日期
				re := regexp.MustCompile(`logs-(\d{4}\.\d{2}\.\d{2})`)
				matches := re.FindStringSubmatch(fileName)
				if len(matches) > 1 {
					dateStr := matches[1]
					date, err := time.Parse(DateFormat, dateStr)
					if err != nil {
						log.Printf("Warning: Failed to parse date from filename %s: %v", fileName, err)
						continue
					}
					if latestDate.IsZero() || date.After(latestDate) {
						latestDate = date
					}
				}
			}
		}
		// 如果找到有效的最新日期，更新 DumpIndexStart
		if !latestDate.IsZero() {
			newStartIndex := fmt.Sprintf(LogsIndexFormat, latestDate.Format(DateFormat))
			log.Printf("Found latest date in files: %s, updating DumpIndexStart", latestDate.Format(DateFormat))
			config.Cfgs.DumpIndexStart = newStartIndex
		}
	}

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
		indexToDump := fmt.Sprintf(LogsIndexFormat, date.Format(DateFormat))
		url := fmt.Sprintf("%s/%s/%s%s", config.Cfgs.DumpESAddr, indexToDump, option, scrollTime)
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf(ErrEncodingJSON, err)
		}
		result := esHttp.ESPost(jsonBody, url)
		scrollRes := &ScrollResponse{}
		if err := json.Unmarshal(result, &scrollRes); err != nil {
			log.Fatalf(ErrDecodingResponseJSON, err)
		}
		HandleBatchData(scrollRes, indexToDump)
	}
	log.Println("dump with batch finish")
}

// HandleBatchData 處理批次資料
func HandleBatchData(scrollRes *ScrollResponse, index string) {
	var batchData []Hit
	batchNum := 1

	// 處理第一批資料
	processBatch(scrollRes.Hits.Hits, &batchData, index, &batchNum)

	// 繼續處理scroll資料
	for len(scrollRes.Hits.Hits) > 0 {
		scrollRes = fetchNextScrollData(scrollRes.ScrollID)
		processBatch(scrollRes.Hits.Hits, &batchData, index, &batchNum)
	}

	// 處理最後一批不足設定條數的資料
	if len(batchData) > 0 {
		writeBatchFile(batchData, index, batchNum)
	}
}

// processBatch 處理單批資料
func processBatch(hits []Hit, batchData *[]Hit, index string, batchNum *int) {
	for k := range hits {
		source := processSource(hits[k])
		resData := createHitData(hits[k], source)
		*batchData = append(*batchData, resData)

		// 當資料達到設定時，寫入檔案
		if len(*batchData) >= config.Cfgs.DumpLenImportData {
			writeBatchFile(*batchData, index, *batchNum)
			*batchData = []Hit{}
			*batchNum++
		}
	}
}

// processSource 處理源數據
func processSource(hit Hit) SourceData {
	source := hit.Source
	source = initializeSource(source, hit.ID)
	source = processTimeFields(source)
	source = processAuditNodes(source)
	source = calculateOver60s(source)
	return source
}

// initializeSource 初始化源數據
func initializeSource(source SourceData, id string) SourceData {
	source.DocType = "fubon"
	source.Pid = id
	if source.Labels == nil {
		source.Labels = []string{}
	}
	return source
}

// processTimeFields 處理時間字段
func processTimeFields(source SourceData) SourceData {
	if startTime, err := time.Parse(TimeFormatInput, source.StartTime); err == nil {
		source.StartTime = startTime.UTC().Format(TimeFormatOutput)
	}
	if endTime, err := time.Parse(TimeFormatInput, source.EndTime); err == nil {
		source.EndTime = endTime.UTC().Format(TimeFormatOutput)
	}
	if modiTime, err := time.Parse(TimeFormatInput, source.ModiTime); err == nil {
		source.ModiTime = modiTime.UTC().Format(TimeFormatOutput)
	}
	if importTime, err := time.Parse(TimeFormatInput, source.ImportTime); err == nil {
		source.ImportTime = importTime.UTC().Format(TimeFormatOutput)
	}
	return source
}

// processAuditNodes 處理審計節點
func processAuditNodes(source SourceData) SourceData {
	auditNodes := []string{"Root"}
	if source.OrgArea != "" {
		auditNodes = append(auditNodes, fmt.Sprintf("Root/%s", source.OrgArea))
		if source.OrgGroup != "" {
			auditNodes = append(auditNodes, fmt.Sprintf("Root/%s/%s", source.OrgArea, source.OrgGroup))
		}
	}
	source.AuditNodes = auditNodes
	return source
}

// calculateOver60s 計算超過60秒標記
func calculateOver60s(source SourceData) SourceData {
	startTime, err := time.Parse(TimeFormatOutput, source.StartTime)
	if err != nil {
		log.Printf("Warning: Failed to parse StartTime: %v", err)
		return source
	}
	endTime, err := time.Parse(TimeFormatOutput, source.EndTime)
	if err != nil {
		log.Printf("Warning: Failed to parse EndTime: %v", err)
		return source
	}

	duration := endTime.Sub(startTime).Seconds()
	if duration > 60 {
		source.Over60s = 1
	} else {
		source.Over60s = 0
	}
	return source
}

// createHitData 創建Hit數據
func createHitData(originalHit Hit, source SourceData) Hit {
	return Hit{
		Index:  originalHit.Index,
		Type:   originalHit.Type,
		ID:     originalHit.ID,
		Score:  originalHit.Score,
		Source: source,
	}
}

// fetchNextScrollData 獲取下一批scroll數據
func fetchNextScrollData(scrollID string) *ScrollResponse {
	url := fmt.Sprintf("%s/_search/scroll", config.Cfgs.DumpESAddr)
	requestBody := map[string]interface{}{
		"scroll":    "5m",
		"scroll_id": scrollID,
	}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatalf(ErrEncodingJSON, err)
	}
	result := esHttp.ESPost(jsonBody, url)
	scrollRes := &ScrollResponse{}
	if err := json.Unmarshal(result, scrollRes); err != nil {
		log.Fatalf(ErrDecodingResponseJSON, err)
	}
	return scrollRes
}

// writeBatchFile 將批次資料寫入檔案
func writeBatchFile(data []Hit, index string, batchNum int) {
	if len(data) == 0 {
		return
	}

	fileName := fmt.Sprintf("%s/%s_%d.json", config.Cfgs.DumpPath, index, batchNum)
	jsonData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		log.Fatalf(ErrEncodingJSON, err)
	}

	err = os.WriteFile(fileName, jsonData, 0644)
	if err != nil {
		log.Fatalf("Error writing to file %s: %v", fileName, err)
	}
	log.Printf("Successfully wrote file %s (records: %d)", fileName, len(data))
}
