package dumpData

var (
	importData []Hit
	i          int = 1
)

type ImportData struct {
	Index      string `json:"_index"`
	Type       string `json:"_type"`
	ID         string `json:"_id"`
	SourceData `bson:",inline"`
}

type ScrollResponse struct {
	ScrollID string `json:"_scroll_id"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shard  `json:"_shards"`
	Hits     Hits   `json:"hits"`
}

type ScrollDataForImport struct {
	Took     int   `json:"took"`
	TimedOut bool  `json:"timed_out"`
	Shards   Shard `json:"_shards"`
	Hits     Hits  `json:"hits"`
}

type Shard struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type Hits struct {
	Total    int     `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Hit struct {
	Index  string     `json:"_index"`
	Type   string     `json:"_type"`
	ID     string     `json:"_id"`
	Score  float64    `json:"_score"`
	Source SourceData `json:"_source"`
}

type SourceData struct {
	DocType              string   `json:"docType"`
	Customer0            string   `json:"customer0"`
	Agent0               string   `json:"agent0"`
	Dialogs              string   `json:"dialogs"`
	VTT                  []string `json:"vtt"`
	StartTime            string   `json:"startTime"`
	EndTime              string   `json:"endTime"`
	ModiTime             string   `json:"modiTime"`
	ImportTime           string   `json:"importTime"`
	Year                 int      `json:"year"`
	Quarter              int      `json:"quarter"`
	Month                int      `json:"month"`
	WeekNum              int      `json:"weekNum"`
	WeekDay              int      `json:"weekDay"`
	MonthDay             int      `json:"monthDay"`
	Length               int      `json:"length"`
	EndStatus            string   `json:"endStatus"`
	ProjectName          string   `json:"projectName"`
	AgentPhoneNo         string   `json:"agentPhoneNo"`
	MixLongestSilence    int      `json:"mixLongestSilence"`
	SilenceGT5Sec        int      `json:"silenceGT5Sec"`
	SilenceGT30Sec       int      `json:"silenceGT30Sec"`
	SilenceRatio         float64  `json:"silenceRatio"`
	R0TotalInterruption  int      `json:"r0TotalInterruption"`
	R1TotalInterruption  int      `json:"r1TotalInterruption"`
	SumTotalInterruption int      `json:"sumTotalInterruption"`
	R0InterruptTimes     int      `json:"r0InterruptTimes"`
	R1InterruptTimes     int      `json:"r1InterruptTimes"`
	MixInterruptTimes    int      `json:"mixInterruptTimes"`
	TalkOverTimeRatio    float64  `json:"talkOverTimeRatio"`
	R0TalkRatio          float64  `json:"r0TalkRatio"`
	R1TalkRatio          float64  `json:"r1TalkRatio"`
	R0SpeakSpeed         float64  `json:"r0SpeakSpeed"`
	R1SpeakSpeed         float64  `json:"r1SpeakSpeed"`
	AgentId              string   `json:"agentId"`
	AgentName            string   `json:"agentName"`
	CallDirection        string   `json:"callDirection"`
	CustomerPhoneNo      string   `json:"customerPhoneNo"`
	CustomerGender       string   `json:"customerGender"`
	OutPhoneNo           string   `json:"outPhoneNo"`
	EEVoiceEmpId         string   `json:"eVoiceEmpId"`
	EEVoiceEmpName       string   `json:"eVoiceEmpName"`
	CustomerName         string   `json:"customerName"`
	CustomerId           string   `json:"customerId"`
	ListCategory         string   `json:"listCategory"`
	OrgArea              string   `json:"orgArea"`
	OrgGroup             string   `json:"orgGroup"`
	ActivityId           string   `json:"activityId"`
	ActivityName         string   `json:"activityName"`
	CallType             string   `json:"callType"`
	QuarterHour          int      `json:"quarterHour"`
	Hour                 int      `json:"hour"`
}
