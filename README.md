# 如意匯出匯入 index 之工具

## 目的
舊如意匯到新如意

## 配置說明

### 匯出(Dump)相關配置
- `dump_es_addr`
  - 說明：舊系統ES位址
  - 預設值：`http://10.85.1.218:30902`

- `dump_index_start`
  - 說明：要撈的起始index
  - 格式：logs-YYYY.MM.DD
  - 預設值：`logs-2024.11.10`
  - 範例：logs-2000.01.01

- `dump_index_end`
  - 說明：要撈的末筆index
  - 格式：logs-YYYY.MM.DD
  - 預設值：`logs-2024.11.10`
  - 範例：logs-2000.01.01

- `dump_post_size`
  - 說明：單次從ES擷取的資料筆數上限
  - 預設值：`1000`
  - 注意：理論上限為10000，建議不要設太高以避免效能問題

- `dump_gte`
  - 說明：資料時間範圍起始
  - 格式：YYYY-MM-DDThh:mm:ss
  - 預設值：`2024-08-13T00:00:00`

- `dump_lte`
  - 說明：資料時間範圍結束
  - 格式：YYYY-MM-DDThh:mm:ss
  - 預設值：`2024-08-15T00:00:00`

- `dump_len_importData`
  - 說明：每個JSON檔案包含的資料筆數
  - 預設值：`5000`
  - ***注意：因為預設的使用情境故此參數沒有作用，可忽略***

- `dump_path`
  - 說明：JSON檔案儲存路徑
  - 預設值：`./JSON`
  - 注意：設定不存在的資料夾路徑會報錯

### 匯入(Import)相關配置 -> 目前只支援新版如意
- `import_es_addr`
  - 說明：新系統ES位址
  - 預設值：`https://admin:admin@localhost:9200`

- `import_index`
  - 說明：新系統索引名稱
  - 預設值：`logs-fubon-000001`

- `import_path`
  - 說明：讀取JSON檔案的路徑
  - 預設值：`./JSON`
  - 注意：設定不存在的資料夾路徑會報錯

- `import_size`
  - 說明：單次寫入ES的資料筆數
  - 預設值：`100`
  - 注意：不建議設定太高

### 執行模式
- `mode`
  - 說明：程式執行模式
  - 預設值：`0`
  - 選項：
    - 0: 僅執行匯出
    - 1: 僅執行匯入
    - 2: 執行匯出及匯入

## 待辦事項
- _bulk refactor
