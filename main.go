package main

import (
	"log"
	"server/app/dumpData"
	"server/app/importData"
	"server/config"
)

func main() {
	config.NewConfig()
	switch config.Cfgs.Mode {
	// "0=匯出"
	case 0:
		log.Println("mode=0, dump data only")
		dumpData.Dump()
	// "1=匯入"
	case 1:
		log.Println("mode=1, import data only")
		importData.Import()
	// "2=匯出加匯入"
	case 2:
		log.Println("mode=2, dump data then import data")
		dumpData.Dump()
		importData.Import()
	}
}
