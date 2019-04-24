package flow

import (
	"context"
	"reflect"
)

type Flow struct {
	in  chan interface{}
	buf []interface{}
}

func NewFlow(in chan interface{}) *Flow {
	return &Flow{
		in: in,
	}
}

func (f *Flow) Chan() chan interface{} {
	return f.in
}

func (f *Flow) PushBuf(val interface{}) {
	f.buf = append(f.buf, val)
}

func (f *Flow) Len() int {
	return len(f.buf) + len(f.in)
}

func (f *Flow) Receive() interface{} {
	if len(f.buf) > 0 {
		val := f.buf[0]
		f.buf[0] = nil
		f.buf = f.buf[1:]
		return val
	}
	return <-f.in
}

func (f *Flow) Send(val interface{}) {
	f.in <- val
}

func getReadyChannels(termChan chan struct{}, chans []*Flow) ([]*Flow, bool) {
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

	index, value, _ := reflect.Select(cases)
	if index > 0 {
		//TODO: handle closed chan
		pickedOne := chans[index-1]
		pickedOne.PushBuf(value.Interface())
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
	//Create an additional channel to make the reflect.Select return
	//when the context expires
	c := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c <- struct{}{}
		}
	}()
	return getReadyChannels(c, chans)
}
