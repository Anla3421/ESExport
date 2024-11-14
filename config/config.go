package config

import (
	"flag"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	Cfgs *Configs
)

type Configs struct {
	DumpESAddr        string
	DumpIndexStart    string
	DumpIndexEnd      string
	DumpPostSize      int
	DumpGte           string
	DumpLte           string
	DumpLenImportData int
	DumpPath          string
	ImportESAddr      string
	ImportIndex       string
	ImportPath        string
	ImportSize        int
	Mode              int
}

func initVariable() {
	flag.Set("alsologtostderr", "true")
	flag.CommandLine.Parse([]string{})
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.String("dump_es_addr", "http://10.85.1.218:30902", "dump elasticserach address")
	pflag.String("dump_index_start", "logs-2016.01.01", "起始 index，範例，logs-2000.01.01")
	pflag.String("dump_index_end", "logs-2016.05.15", "結束 index，範例，logs-2000.01.01")
	pflag.Int("dump_post_size", 1000, "單次撈 es 取得的筆數上限，理論上限是 10000，但不要設太高，會有效能跟容量撞上限導致報錯的問題")
	pflag.String("dump_gte", "2016-01-01T00:00:00", "gte")
	pflag.String("dump_lte", "2016-07-15T23:59:59", "lte")
	pflag.Int("dump_len_importData", 30000, "多少筆做一次寫入檔案，即一個.json內會有幾筆資料")
	pflag.String("dump_path", "./JSON", "要建立檔案的位置，設不存在的資料夾會報錯，若有需要設定，再改code")
	pflag.String("import_es_addr", "https://admin:admin@localhost:9200", "dump elasticserach address，目前只有支援新版如意")
	pflag.String("import_index", "logs-fubon-000001", "新版如意使用，目前只有支援新版如意es")
	pflag.String("import_path", "./JSON", "要讀的檔案位置，設不存在的資料夾會報錯，若有需要設定，再改code")
	pflag.Int("import_size", 100, "多少筆做一次寫入ES的動作，不建議設太高，k >= 99")
	pflag.Int("mode", 0, "0=匯出，1=匯入，2=匯出加匯入")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
}

func NewConfig() {
	initVariable()
	Cfgs = &Configs{
		DumpESAddr:        viper.GetString("dump_es_addr"),
		DumpIndexStart:    viper.GetString("dump_index_start"),
		DumpIndexEnd:      viper.GetString("dump_index_end"),
		DumpPostSize:      viper.GetInt("dump_post_size"),
		DumpGte:           viper.GetString("dump_gte"),
		DumpLte:           viper.GetString("dump_lte"),
		DumpLenImportData: viper.GetInt("dump_len_importData"),
		DumpPath:          viper.GetString("dump_path"),
		ImportESAddr:      viper.GetString("import_es_addr"),
		ImportIndex:       viper.GetString("import_index"),
		ImportPath:        viper.GetString("import_path"),
		ImportSize:        viper.GetInt("import_size"),
		Mode:              viper.GetInt("mode"),
	}
}
