// Package drr provides a simple generic implementation of Deficit Round Robin
// scheduler for channels.
package drr

import (
	"context"
	"errors"
	"reflect"
)

var (
	// ErrInvalidPriorityValue error is returned by Input method when
	// priority value is less than or equal to 0.
	ErrInvalidPriorityValue = errors.New("ErrInvalidPriorityValue")
	// ErrChannelIsNil error is returned by NewDRR and Input methods
	// when channel is nil.
	ErrChannelIsNil = errors.New("ErrChannelIsNil")
	// ErrContextIsNil is returned by Start method when context.Context
	// is nil
	ErrContextIsNil = errors.New("ContextIsNil")
)

type flow[T any] struct {
	c    <-chan T
	prio int
}

// DRR is a Deficit Round Robin scheduler, as detailed in
// https://en.wikipedia.org/wiki/Deficit_round_robin.
type DRR[T any] struct {
	flows         []flow[T]
	outChan       chan T
	flowsToDelete []int
}

// NewDRR creates a new DRR with indicated output channel.
//
// The outChan must be non-nil, otherwise NewDRR returns
// ErrChannelIsNil error.
func NewDRR[T any](outChan chan T) (*DRR[T], error) {
	if outChan == nil {
		return nil, ErrChannelIsNil
	}
	return &DRR[T]{
		outChan: outChan,
	}, nil
}

// Input registers a new ingress flow, that is a channel with
// priority.
//
// Input returns ErrChannelIsNil if input channel is nil.
// Priority must be greater than 0, otherwise Input returns
// ErrInvalidPriorityValue error.
func (d *DRR[T]) Input(prio int, in <-chan T) error {
	if prio <= 0 {
		return ErrInvalidPriorityValue
	}
	if in == nil {
		return ErrChannelIsNil
	}
	d.flows = append(d.flows, flow[T]{c: in, prio: prio})
	return nil
}

// Start actually spawns the DRR goroutine. Once Start is called,
// the goroutine starts forwarding from input channels previously registered
// through Input method to output channel.
//
// Start returns ContextIsNil error if ctx is nil.
//
// DRR goroutine exits when context.Context expires or when all the input
// channels are closed. DRR goroutine closes the output channel upon termination.
func (d *DRR[T]) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrContextIsNil
	}
	go func() {
		defer close(d.outChan)
		for {
			// Wait for at least one channel to be ready
			readyIndex, value, ok := d.getReadyChannel(
				ctx,
				d.flows)
			if readyIndex < 0 {
				// Context expired, exit
				return
			}
		flowLoop:
			for index, flow := range d.flows {
				dc := flow.prio
				if readyIndex == index {
					if !ok {
						// Chan got closed, remove it from internal slice
						d.prepareToUnregister(index)
						continue flowLoop
					} else {
						// This chan triggered the reflect.Select statement
						// transmit its value and decrement its deficit counter
						d.outChan <- value
						dc = flow.prio - 1
					}
				}
				// Trasmit from channel until it has nothing else to send
				// or its DC reaches 0
				for i := 0; i < dc; i++ {
					//First, check if context expired
					select {
					case <-ctx.Done():
						// Context expired, exit
						return
					default:
					}
					//Then, read from input chan
					select {
					case val, ok := <-flow.c:
						if !ok {
							// Chan got closed, remove it from internal slice
							d.prepareToUnregister(index)
							continue flowLoop
						} else {
							d.outChan <- val
						}
					default:
						continue flowLoop
					}
				}
			}
			// All channel closed in this execution can now be actually removed
			last := d.unregisterFlows()
			if last {
				return
			}
		}
	}()
	return nil
}

func (d *DRR[T]) prepareToUnregister(index int) {
	d.flowsToDelete = append(d.flowsToDelete, index)
}

func (d *DRR[T]) unregisterFlows() bool {
	oldFlows := d.flows
	d.flows = make([]flow[T], 0, len(oldFlows)-len(d.flowsToDelete))
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

func (d *DRR[T]) getReadyChannel(ctx context.Context, flows []flow[T]) (int, T, bool) {
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
		var zeroT T
		return -1, zeroT, false
	}
	//Rescaling index (-1) because of additional termination channel
	return index - 1, value.Interface().(T), ok
}
