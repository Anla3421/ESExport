package esHttp

import (
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

var httpClient *http.Client

func init() {
	transport := &http.Transport{
		// 忽略證書驗證，用於自簽憑證的環境
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		// 設定連線池
		MaxIdleConns:        200,              // 增加最大空閒連線數
		MaxIdleConnsPerHost: 100,              // 每個 host 的最大空閒連線數
		IdleConnTimeout:     90 * time.Second, // 空閒連線超時時間
		DisableKeepAlives:   false,            // 啟用 keep-alive
		// TCP 連線相關設定
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // 連線超時
			KeepAlive: 30 * time.Second, // keep-alive 間隔
			DualStack: true,             // 啟用 IPv4/IPv6
		}).DialContext,
		MaxConnsPerHost:    100,   // 限制每個 host 的最大連線數
		ForceAttemptHTTP2:  false, // 禁用 HTTP/2
		DisableCompression: true,  // 禁用壓縮
	}

	httpClient = &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second, // 請求超時時間
	}
}

func ESPost(requestBody []byte, url string) []byte {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = false // 允許連線重用

	// 重試機制
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Printf("Attempt %d: Error sending request: %v", i+1, err)
			if i == maxRetries-1 {
				log.Fatalf("Failed after %d attempts: %v", maxRetries, err)
			}
			// 指數退避，每次重試間隔加倍
			backoff := time.Duration(1<<uint(i)) * time.Second
			time.Sleep(backoff)
			continue
		}
		defer resp.Body.Close()

		// 讀取回應內容
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Error reading response body: %v", err)
		}

		// 檢查回應狀態碼
		if resp.StatusCode >= 400 {
			log.Printf("Server returned error status: %d - %s", resp.StatusCode, string(body))
			if i == maxRetries-1 {
				log.Fatalf("Failed after %d attempts with status code %d", maxRetries, resp.StatusCode)
			}
			continue
		}

		return body
	}
	return nil
}
