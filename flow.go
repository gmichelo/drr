package drr

import (
	"context"
	"reflect"

	"github.com/gammazero/deque"
)

type Flow struct {
	id, prio int
	in       chan interface{}
	buf      deque.Deque
	closed   bool
}

func NewFlow(id, prio int, in chan interface{}) *Flow {
	return &Flow{
		id:   id,
		prio: prio,
		in:   in,
	}
}

func (f *Flow) Chan() chan interface{} {
	return f.in
}

func (f *Flow) Len() int {
	return f.buf.Len() + len(f.in)
}

func (f *Flow) Receive() (interface{}, bool) {
	if f.buf.Len() > 0 {
		val := f.buf.PopFront()
		return val, !f.Closed()
	}
	return <-f.in, !f.Closed()
}

func (f *Flow) Send(val interface{}) {
	f.in <- val
}

func (f *Flow) pushBuf(val interface{}) {
	f.buf.PushBack(val)
}

func (f *Flow) close() {
	f.closed = true
}

func (f *Flow) Priority() int {
	return f.prio
}

func (f *Flow) ID() int {
	return f.id
}

func (f *Flow) Closed() bool {
	return f.buf.Len() == 0 && f.closed
}

func getReadyChannels(termChan <-chan struct{}, chans []*Flow) ([]*Flow, bool) {
	var res []*Flow
	var cases []reflect.SelectCase
	//First case is the termiantion channel for context cancellation
	c := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(termChan),
	}
	cases = append(cases, c)
	//Create list of SelectCase
	for _, ch := range chans {
		c := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.Chan()),
		}
		cases = append(cases, c)
	}

	index, value, ok := reflect.Select(cases)
	if index > 0 {
		//Rescaling index (-1) because of additional termination channel
		pickedOne := chans[index-1]
		pickedOne.pushBuf(value.Interface())
		if !ok {
			//Channel closed
			pickedOne.close()
		}
	}
	//Loop over all channels (flows) and add the ones
	//that are not empty
	for _, f := range chans {
		if f.Len() > 0 {
			res = append(res, f)
		}
	}
	//If list of ready channels is 0 and the
	//index of activated channel is 0
	//then it means that the context expired
	if len(res) == 0 && index == 0 {
		return nil, false
	}
	return res, true
}

func GetReadyChannels(ctx context.Context, chans []*Flow) ([]*Flow, bool) {
	return getReadyChannels(ctx.Done(), chans)
}
