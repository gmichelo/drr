package flow

import (
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

func getReadyChannels(chans []*Flow) []*Flow {
	var res []*Flow
	var cases []reflect.SelectCase
	//Create list of SelectCase
	for _, ch := range chans {
		c := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.Chan()),
		}
		cases = append(cases, c)
	}

	index, value, _ := reflect.Select(cases)
	//TODO: handle closed chan
	pickedOne := chans[index]
	pickedOne.PushBuf(value)

	for _, f := range chans {
		if f.Len() > 0 {
			res = append(res, f)
		}
	}
	return res
}
