/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package producer

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	topic          = "TopicTest"
	namespaceTopic = "Test%TopicTest"
)

func TestShutdown(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := internal.NewMockRMQClient(ctrl)
	p.client = client

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	client.EXPECT().Shutdown().Return()
	client.EXPECT().UnregisterProducer(gomock.Any()).Return()
	err = p.Shutdown()
	assert.Nil(t, err)

	ctx := context.Background()
	msg := new(primitive.Message)

	r, err := p.SendSync(ctx, msg)
	assert.Equal(t, errors.ErrNotRunning, err)
	assert.Nil(t, r)

	err = p.SendOneWay(ctx, msg)
	assert.Equal(t, errors.ErrNotRunning, err)

	f := func(context.Context, *primitive.SendResult, error) {
		assert.False(t, true, "should not  come in")
	}
	err = p.SendAsync(ctx, f, msg)
	assert.Equal(t, errors.ErrNotRunning, err)
}

func mockB4Send(p *defaultProducer) {
	p.publishInfo.Store(topic, &internal.TopicPublishInfo{
		HaveTopicRouterInfo: true,
		MqList: []*primitive.MessageQueue{
			{
				Topic:      topic,
				BrokerName: "aa",
				QueueId:    0,
			},
		},
	})
	p.publishInfo.Store(namespaceTopic, &internal.TopicPublishInfo{
		HaveTopicRouterInfo: true,
		MqList: []*primitive.MessageQueue{
			{
				Topic:      namespaceTopic,
				BrokerName: "aa",
				QueueId:    0,
			},
		},
	})
	p.options.Namesrv.AddBroker(&internal.TopicRouteData{
		BrokerDataList: []*internal.BrokerData{
			{
				Cluster:    "cluster",
				BrokerName: "aa",
				BrokerAddresses: map[int64]string{
					0: "1",
				},
			},
		},
	})
}

func TestSync(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	namesrvCli := internal.NewMockNamesrvs(ctrl)
	client := internal.NewMockRMQClient(ctrl)
	p.client = client
	client.SetNameSrv(namesrvCli)
	namesrvCli.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("a")

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	ctx := context.Background()
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte("this is a message body"),
		Queue: &primitive.MessageQueue{
			Topic:      topic,
			BrokerName: "aa",
			QueueId:    0,
		},
	}
	msg.WithProperty("key", "value")

	expectedResp := &primitive.SendResult{
		Status:      primitive.SendOK,
		MsgID:       "111",
		QueueOffset: 0,
		OffsetMsgID: "0",
	}

	mockB4Send(p)

	cmd := &remote.RemotingCommand{
		Code: internal.ResSuccess,
	}

	client.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cmd, nil)
	client.EXPECT().ProcessSendResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) {
			resp.Status = expectedResp.Status
			resp.MsgID = expectedResp.MsgID
			resp.QueueOffset = expectedResp.QueueOffset
			resp.OffsetMsgID = expectedResp.OffsetMsgID
		})
	resp, err := p.SendSync(ctx, msg)
	assert.Nil(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestASync(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	namesrvCli := internal.NewMockNamesrvs(ctrl)
	client := internal.NewMockRMQClient(ctrl)
	p.client = client
	client.SetNameSrv(namesrvCli)
	namesrvCli.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("a")

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	ctx := context.Background()
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte("this is a message body"),
		Queue: &primitive.MessageQueue{
			Topic:      topic,
			BrokerName: "aa",
			QueueId:    0,
		},
	}
	msg.WithProperty("key", "value")

	expectedResp := &primitive.SendResult{
		Status:      primitive.SendOK,
		MsgID:       "111",
		QueueOffset: 0,
		OffsetMsgID: "0",
	}

	f := func(ctx context.Context, resp *primitive.SendResult, err error) {
		assert.Nil(t, err)
		assert.Equal(t, expectedResp, resp)
	}

	mockB4Send(p)

	client.EXPECT().InvokeAsync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, addr string, request *remote.RemotingCommand,
			f func(*remote.RemotingCommand, error)) error {
			// mock invoke callback
			f(nil, nil)
			return nil
		})
	client.EXPECT().ProcessSendResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) {
			resp.Status = expectedResp.Status
			resp.MsgID = expectedResp.MsgID
			resp.QueueOffset = expectedResp.QueueOffset
			resp.OffsetMsgID = expectedResp.OffsetMsgID
		})

	err = p.SendAsync(ctx, f, msg)
	assert.Nil(t, err)
}

func TestOneway(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	namesrvCli := internal.NewMockNamesrvs(ctrl)
	client := internal.NewMockRMQClient(ctrl)
	p.client = client
	client.SetNameSrv(namesrvCli)
	namesrvCli.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("a")

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	ctx := context.Background()
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte("this is a message body"),
		Queue: &primitive.MessageQueue{
			Topic:      topic,
			BrokerName: "aa",
			QueueId:    0,
		},
	}
	msg.WithProperty("key", "value")

	mockB4Send(p)

	client.EXPECT().InvokeOneWay(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	err = p.SendOneWay(ctx, msg)
	assert.Nil(t, err)
}

func TestSyncWithNamespace(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
		WithNamespace("Test"),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	namesrvCli := internal.NewMockNamesrvs(ctrl)
	client := internal.NewMockRMQClient(ctrl)
	p.client = client
	client.SetNameSrv(namesrvCli)
	namesrvCli.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("a")

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	ctx := context.Background()
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte("this is a message body"),
		Queue: &primitive.MessageQueue{
			Topic:      namespaceTopic,
			BrokerName: "aa",
			QueueId:    0,
		},
	}
	msg.WithProperty("key", "value")

	expectedResp := &primitive.SendResult{
		Status:      primitive.SendOK,
		MsgID:       "111",
		QueueOffset: 0,
		OffsetMsgID: "0",
	}

	mockB4Send(p)

	cmd := &remote.RemotingCommand{
		Code: internal.ResSuccess,
	}

	client.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(cmd, nil)
	client.EXPECT().ProcessSendResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) {
			resp.Status = expectedResp.Status
			resp.MsgID = expectedResp.MsgID
			resp.QueueOffset = expectedResp.QueueOffset
			resp.OffsetMsgID = expectedResp.OffsetMsgID
		})
	resp, err := p.SendSync(ctx, msg)
	assert.Nil(t, err)
	assert.Equal(t, expectedResp, resp)
	assert.Equal(t, namespaceTopic, msg.Topic)
}

func TestBatchSendDifferentTopics(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithRetry(2),
		WithQueueSelector(NewManualQueueSelector()),
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := internal.NewMockRMQClient(ctrl)
	p.client = client

	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(nil)
	client.EXPECT().Start().Return()
	err := p.Start()
	assert.Nil(t, err)

	ctx := context.Background()
	msgToA := &primitive.Message{
		Topic: "topic-A",
		Body:  []byte("this is a message body"),
	}

	msgToB := &primitive.Message{
		Topic: "topic-B",
		Body:  []byte("this is a message body"),
	}

	resp, err := p.SendSync(ctx, []*primitive.Message{msgToA, msgToB}...)
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.Equal(t, err, errors.ErrMultipleTopics)
}

func TestSendMessageIDs(t *testing.T) {
	for _, tt := range []struct {
		name        string
		async       bool
		intercepted bool
		count       int
	}{
		{name: "sync/single", count: 1},
		{name: "sync/batch", count: 3},
		{name: "async/single", async: true, count: 1},
		{name: "async/batch", async: true, count: 3},
		{name: "sync/interceptor/single", intercepted: true, count: 1},
		{name: "sync/interceptor/batch", intercepted: true, count: 3},
		{name: "async/interceptor/single", async: true, intercepted: true, count: 1},
		{name: "async/interceptor/batch", async: true, intercepted: true, count: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewDefaultProducer(
				WithInstanceName(t.Name()),
				WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
				WithQueueSelector(NewManualQueueSelector()),
			)
			require.NoError(t, err)
			realClient := p.client
			defer realClient.Shutdown()
			p.state = int32(internal.StateRunning)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := internal.NewMockRMQClient(ctrl)
			namesrv := internal.NewMockNamesrvs(ctrl)
			client.SetNameSrv(namesrv)
			p.client = client
			namesrv.EXPECT().FindBrokerAddrByName("aa").Return("broker")
			mockB4Send(p)

			interceptorCalled := false
			if tt.intercepted {
				p.interceptor = func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
					interceptorCalled = true
					return next(ctx, req, reply)
				}
			}

			msgs := make([]*primitive.Message, tt.count)
			expectedMsgs := make([]interface{}, tt.count)
			for i := range msgs {
				msgs[i] = primitive.NewMessage(topic, []byte(fmt.Sprintf("message-%d", i)))
				msgs[i].Queue = &primitive.MessageQueue{Topic: topic, BrokerName: "aa"}
				expectedMsgs[i] = msgs[i]
			}
			msgs[0].WithProperty(primitive.PropertyUniqueClientMessageIdKeyIndex, "existing-id")

			cmd := &remote.RemotingCommand{
				Code: internal.ResSuccess,
				ExtFields: map[string]string{
					"msgId":       "offset-id",
					"queueId":     "0",
					"queueOffset": "42",
				},
			}
			client.EXPECT().ProcessSendResponse("aa", cmd, gomock.Any(), expectedMsgs...).
				DoAndReturn(realClient.ProcessSendResponse)

			checkRequest := func(request *remote.RemotingCommand) {
				expectedCode := internal.ReqSendMessage
				if tt.count > 1 {
					expectedCode = internal.ReqSendBatchMessage
				}
				assert.Equal(t, expectedCode, request.Code)
			}

			var result *primitive.SendResult
			if tt.async {
				client.EXPECT().InvokeAsync(gomock.Any(), "broker", gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, addr string, request *remote.RemotingCommand, callback func(*remote.RemotingCommand, error)) error {
						checkRequest(request)
						callback(cmd, nil)
						return nil
					})
				err = p.SendAsync(context.Background(), func(ctx context.Context, resp *primitive.SendResult, sendErr error) {
					assert.NoError(t, sendErr)
					result = resp
				}, msgs...)
			} else {
				client.EXPECT().InvokeSync(gomock.Any(), "broker", gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, addr string, request *remote.RemotingCommand, timeout time.Duration) (*remote.RemotingCommand, error) {
						checkRequest(request)
						return cmd, nil
					})
				result, err = p.SendSync(context.Background(), msgs...)
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			ids := make([]string, tt.count)
			for i, msg := range msgs {
				ids[i] = msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
				require.NotEmpty(t, ids[i])
			}
			assert.Equal(t, "existing-id", ids[0])
			if tt.count > 1 {
				assert.NotEqual(t, ids[1], ids[2])
			}
			assert.Equal(t, strings.Join(ids, ","), result.MsgID)
			assert.Equal(t, "offset-id", result.OffsetMsgID)
			assert.Equal(t, int64(42), result.QueueOffset)
			assert.Equal(t, tt.intercepted, interceptorCalled)
		})
	}
}

func TestMarshalMessageBatchIDs(t *testing.T) {
	msgs := []*primitive.Message{
		primitive.NewMessage(topic, []byte("first")),
		primitive.NewMessage(topic, []byte("second")),
		primitive.NewMessage(topic, []byte("third")),
	}
	msgs[0].WithProperty(primitive.PropertyUniqueClientMessageIdKeyIndex, "existing-id")

	body := MarshalMessageBatch(msgs...)
	ids := make([]string, len(msgs))
	for i, msg := range msgs {
		require.GreaterOrEqual(t, len(body), 22)
		size := int(binary.BigEndian.Uint32(body))
		require.LessOrEqual(t, size, len(body))
		bodySize := int(binary.BigEndian.Uint32(body[16:]))
		require.GreaterOrEqual(t, size, 22+bodySize)
		assert.Equal(t, msg.Body, body[20:20+bodySize])
		decoded := new(primitive.Message)
		decoded.UnmarshalProperties(body[22+bodySize : size])
		ids[i] = msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
		require.NotEmpty(t, ids[i])
		assert.Equal(t, ids[i], decoded.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex))
		body = body[size:]
	}
	assert.Empty(t, body)
	assert.Equal(t, "existing-id", ids[0])
	assert.NotEqual(t, ids[1], ids[2])

	MarshalMessageBatch(msgs...)
	for i, msg := range msgs {
		assert.Equal(t, ids[i], msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex))
	}
}
