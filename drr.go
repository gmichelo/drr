package drr

import (
	"context"
	"reflect"
)

type flow struct {
	c    chan interface{}
	prio int
}

type DRR struct {
	flows         []flow
	outChan       chan interface{}
	flowsToDelete []int
}

func NewDRR(outChan chan interface{}) *DRR {
	return &DRR{
		outChan: outChan,
	}
}

func (d *DRR) Output() chan interface{} {
	return d.outChan
}

func (d *DRR) Input(prio int, in chan interface{}) {
	//TODO: validate input prio and chan
	//zero or negative priority is not allowed as that flow
	//would never be scheduled and nil channel would do the same
	d.flows = append(d.flows, flow{c: in, prio: prio})
}

func (d *DRR) Start(ctx context.Context) {
	go func() {
		defer close(d.outChan)
		for {
			//TODO Review context generation and propagation
			readyIndex, value, ok := getReadyChannel(
				ctx,
				d.flows)
			if readyIndex < 0 {
				return
			}
		flowLoop:
			for index, flow := range d.flows {
				dc := flow.prio
				if readyIndex == index {
					if !ok {
						d.prepareToUnregister(index)
						continue flowLoop
					} else {
						d.outChan <- value
						dc = flow.prio - 1
					}
				}
				for i := 0; i < dc; i++ {
					select {
					case val, ok := <-flow.c:
						if !ok {
							d.prepareToUnregister(index)
							continue flowLoop
						} else {
							d.outChan <- val
						}
					case <-ctx.Done():
						return
					default:
						continue flowLoop
					}
				}
			}
			last := d.unregisterFlows()
			if last {
				return
			}
		}
	}()
}

func (d *DRR) prepareToUnregister(index int) {
	d.flowsToDelete = append(d.flowsToDelete, index)
}

func (d *DRR) unregisterFlows() bool {
	oldFlows := d.flows
	d.flows = make([]flow, 0, len(oldFlows)-len(d.flowsToDelete))
oldFlowsLoop:
	for i, flow := range oldFlows {
		for _, index := range d.flowsToDelete {
			if index == i {
				continue oldFlowsLoop
			}
		}
		d.flows = append(d.flows, flow)
	}
	d.flowsToDelete = []int{}
	return len(d.flows) == 0
}

func getReadyChannel(ctx context.Context, flows []flow) (int, interface{}, bool) {
	cases := make([]reflect.SelectCase, 0, len(flows)+1)
	//First case is the termiantion channel for context cancellation
	c := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	cases = append(cases, c)
	//Create list of SelectCase
	for _, f := range flows {
		c := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(f.c),
		}
		cases = append(cases, c)
	}
	//Call Select on all channels
	index, value, ok := reflect.Select(cases)
	//Termination channel
	if index == 0 {
		return -1, nil, false
	}
	//Rescaling index (-1) because of additional termination channel
	return index - 1, value.Interface(), ok
}
