package main

import (
	"estool/app/dumpData"
	"estool/app/importData"
	"estool/config"
	"log"
)

func main() {
	config.NewConfig()
	switch config.Cfgs.Mode {
	// "0=匯出"
	case 0:
		log.Printf("mode=0, dump data only, from %s\n", config.Cfgs.DumpESAddr)
		dumpData.DumpWithBatch()
	// "1=匯入"
	case 1:
		log.Printf("mode=1, import data only to %s\n", config.Cfgs.ImportESAddr)
		importData.ImportByOpenSearchBulk()
	// "2=匯出加匯入"
	case 2:
		log.Println("mode=2, dump data then import data")
		dumpData.DumpWithBatch()
		importData.ImportByOpenSearchBulk()
	}
}
