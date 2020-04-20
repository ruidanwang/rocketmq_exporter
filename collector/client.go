package collector

import (
	"bytes"
	"sync"
)

type GetRouteInfoRequestHeader struct {
	topic string
}

func (self *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{\"topic\":\"")
	buf.WriteString(self.topic)
	buf.WriteString("\"}")
	return buf.Bytes(), nil
}

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int32
	TopicSynFlag   int32
}

type BrokerData struct {
	BrokerName      string
	BrokerAddrs     map[string]string
	BrokerAddrsLock sync.RWMutex
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

type MqClient struct {
	clientId string

	brokerAddrTable     map[string]map[string]string //map[brokerName]map[bokerId]addrs
	brokerAddrTableLock sync.RWMutex

	consumerTableLock   sync.RWMutex
	topicRouteTable     map[string]*TopicRouteData
	topicRouteTableLock sync.RWMutex
	RemotingClient      RemotingClient
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable: make(map[string]map[string]string),
		topicRouteTable: make(map[string]*TopicRouteData),
	}
}

type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string
}
