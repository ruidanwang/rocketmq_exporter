package collector

import (
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/gjson"
	"strings"
	"sync/atomic"
)

type topicOffsetCollector struct {
	entries *prometheus.Desc
	logger  log.Logger
}

type TopicList struct {
	TopicList []string `json:"topicList"`
}

func init() {
	registerCollector("topic_offset", defaultEnabled, NewTopicOffsetCollector)
}

func NewTopicOffsetCollector(logger log.Logger) (Collector, error) {
	return &topicOffsetCollector{
		entries: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "topic_offset", "entries"),
			"topic_offset entries by topic_offset",
			[]string{"topic_offset"}, nil,
		),
		logger: logger,
	}, nil
}

func getTopicOffsetEntries() (map[string]int64, error) {
	var entries = make(map[string]int64)
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:     GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
		Language: "JAVA",
		Version:  79,
		Opaque:   currOpaque,
		Flag:     0,
		//ExtFields: header,
	}

	response, err := RemotingClient.InvokeSync(NewDefaultRemotingClient(), "10.0.56.28:9876", remotingCommand, 2000)
	if err != nil {
		glog.Error(err)
	}
	var topicList []gjson.Result
	if response.Code == SUCCESS {
		topicList = gjson.Get(string(response.Body), "topicList").Array()
	}

	for _, value := range topicList {

		m, err2 := getTopicStats(value, response, err, entries)
		if err2 != nil {
			return m, err2
		}
	}

	return entries, nil
}

func getTopicStats(value gjson.Result, response *RemotingCommand, err error, entries map[string]int64) (map[string]int64, error) {
	requestHeader := &GetRouteInfoRequestHeader{
		topic: value.String(),
	}

	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = GET_ROUTEINTO_BY_TOPIC
	remotingCommand.ExtFields = requestHeader
	response, err = RemotingClient.InvokeSync(NewDefaultRemotingClient(), "10.0.56.28:9876", remotingCommand, 2000)
	if err != nil {
		glog.Error(err)
		//return 0, err
	}
	if response.Code == SUCCESS {
		topicRouteData := new(TopicRouteData)
		bodyjson := strings.Replace(string(response.Body), ",0:", ",\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1)
		bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), topicRouteData)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		for _, brokerData := range topicRouteData.BrokerDatas {
			for _, addrStr := range brokerData.BrokerAddrs {
				topicStatsInfoCmd := new(RemotingCommand)
				topicStatsInfoCmd.Code = GET_TOPIC_STATS_INFO
				topicStatsInfoCmd.ExtFields = requestHeader
				topicStatsInfoResp, err := RemotingClient.InvokeSync(NewDefaultRemotingClient(), addrStr, topicStatsInfoCmd, 2000)
				if err != nil {
					glog.Error(err)
					//return 0, err
				}

				bodyjson = strings.Replace(string(topicStatsInfoResp.Body), `{"offsetTable":{`, ``, -1)
				bodyjson = strings.Replace(bodyjson, `}}`, ``, -1)
				topArr := strings.Split(bodyjson, `},{`)
				var totalMaxOffset, lastUpdateTimestamp int64
				var borkerStrs string
				for _, topStr := range topArr {
					if !strings.HasPrefix(topStr, "{") {
						topStr = `{"topicOffset":{` + topStr
					} else {
						topStr = `{"topicOffset":` + topStr
					}
					if !strings.HasSuffix(topStr, "}") {
						topStr += "}}"
					} else {
						topStr += "}"
					}
					topStr = strings.Replace(topStr, `}:{`, `},"messageQueue":{`, 1)
					totalMaxOffset = gjson.Get(topStr, `messageQueue.maxOffset`).Int()
					if gjson.Get(topStr, `messageQueue.lastUpdateTimestamp`).Int() > lastUpdateTimestamp {
						lastUpdateTimestamp = gjson.Get(topStr, `messageQueue.lastUpdateTimestamp`).Int()
					}
					strings.Join([]string{borkerStrs, gjson.Get(topStr, `topicOffset.brokerName`).String()}, " ")
				}

				entries[value.String()] = totalMaxOffset
			}
		}
	}
	return entries, nil
}

func (c *topicOffsetCollector) Update(ch chan<- prometheus.Metric) error {
	entries, err := getTopicOffsetEntries()
	if err != nil {
		return fmt.Errorf("could not get ARP entries: %s", err)
	}

	for topicOffset, entryCount := range entries {
		ch <- prometheus.MustNewConstMetric(
			c.entries, prometheus.GaugeValue, float64(entryCount), topicOffset)
	}

	return nil
}
