package flow

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewFlow(t *testing.T) {

	Convey("TestNewFlow", t, func() {
		in := make(chan interface{}, 10)
		f := NewFlow(in)
		So(f, ShouldNotEqual, nil)
		So(f.Len(), ShouldEqual, 0)
	})

}

func TestSend(t *testing.T) {
	in := make(chan interface{}, 10)
	f := NewFlow(in)

	Convey("TestSend", t, func() {
		f.Send("TestMsg")
		So(f.Len(), ShouldEqual, 1)
	})

	Convey("TestOutbound", t, func() {
		in <- "TestMsg"
		So(f.Len(), ShouldEqual, 2)
	})

}

func TestRecv(t *testing.T) {

	Convey("TestRecv", t, func() {
		in := make(chan interface{}, 10)
		f := NewFlow(in)
		payload := "TestMsg"
		f.Send(payload)
		res := f.Receive()
		So(res, ShouldEqual, payload)
	})

}

func TestPushBuf(t *testing.T) {
	in := make(chan interface{}, 10)
	f := NewFlow(in)
	payload1 := "TestMsg1"
	payload2 := "TestMsg2"

	Convey("TestPushBuf", t, func() {
		f.Send(payload1)
		f.Send(payload2)
		res := <-in
		So(res, ShouldEqual, payload1)
		f.pushBuf(res)
		res = f.Receive()
		So(res, ShouldEqual, payload1)
		res = f.Receive()
		So(res, ShouldEqual, payload2)
	})
}

func TestGetReadyChannels(t *testing.T) {
	in1 := make(chan interface{}, 10)
	f1 := NewFlow(in1)
	payload1 := "TestMsg1"
	in2 := make(chan interface{}, 10)
	f2 := NewFlow(in2)
	payload2 := "TestMsg2"
	in3 := make(chan interface{}, 10)
	f3 := NewFlow(in3)
	payload3 := "TestMsg3"

	Convey("TestOneFlowReady", t, func() {
		in2 <- payload2
		res, valid := GetReadyChannels(
			context.Background(),
			[]*Flow{
				f1,
				f2,
				f3,
			},
		)
		So(valid, ShouldEqual, true)
		So(len(res), ShouldEqual, 1)
		out := res[0].Receive()
		str, ok := out.(string)
		So(ok, ShouldEqual, true)
		So(str, ShouldEqual, payload2)
	})

	Convey("TestTwoFlowsReady", t, func() {
		in1 <- payload1
		in3 <- payload3
		res, valid := GetReadyChannels(
			context.Background(),
			[]*Flow{
				f1,
				f2,
				f3,
			},
		)
		So(valid, ShouldEqual, true)
		So(len(res), ShouldEqual, 2)
		for _, flow := range res {
			out := flow.Receive()
			str, ok := out.(string)
			So(ok, ShouldEqual, true)
			So(str, ShouldBeIn, []string{payload1, payload3})
		}
	})

	Convey("TestZeroFlowReady", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		res, valid := GetReadyChannels(
			ctx,
			[]*Flow{
				f1,
				f2,
				f3,
			},
		)
		So(valid, ShouldEqual, false)
		So(res, ShouldBeNil)
	})
}

func BenchmarkSend(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	f := NewFlow(in)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Send("TestMsg")
	}
}

func BenchmarkRecv(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	f := NewFlow(in)
	for i := 0; i < b.N; i++ {
		in <- "TestMsg"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg = f.Receive()
	}
}

func BenchmarkRecvWithPush(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	f := NewFlow(in)
	for i := 0; i < b.N; i++ {
		f.pushBuf("TestMsg")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg = f.Receive()
	}
}

func BenchmarkPushBuf(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	f := NewFlow(in)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.pushBuf("TestMsg")
	}
}

func BenchmarkSendChan(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in <- "TestMsg"
	}
}

var msg interface{}

func BenchmarkRecvChan(b *testing.B) {
	in := make(chan interface{}, b.N+1)
	for i := 0; i < b.N; i++ {
		in <- "TestMsg"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg = <-in
	}
}
