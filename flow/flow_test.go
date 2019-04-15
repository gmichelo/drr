package flow

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
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
		f.PushBuf(res)
		res = f.Receive()
		So(res, ShouldEqual, payload1)
		res = f.Receive()
		So(res, ShouldEqual, payload2)
	})
}
