package drr

import (
	"context"
)

type DRR struct {
	flows   map[int]*Flow
	lastID  int
	outChan chan interface{}
}

func NewDRR(outChan chan interface{}) *DRR {
	return &DRR{
		flows:   make(map[int]*Flow),
		outChan: outChan,
	}
}

func (d *DRR) Output() chan interface{} {
	return d.outChan
}

func (d *DRR) Input(prio int, in chan interface{}) {
	d.flows[d.lastID] = NewFlow(d.lastID, prio, in)
	d.lastID++
}

func mapToSlice(mapIn map[int]*Flow) []*Flow {
	outSlice := make([]*Flow, 0, len(mapIn))
	for _, elem := range mapIn {
		outSlice = append(outSlice, elem)
	}
	return outSlice
}

func (d *DRR) Start() {
	go func() {
		for {
			//TODO Review context generation and propagation
			readyFlows, valid := GetReadyChannels(
				context.TODO(),
				mapToSlice(d.flows))
			if !valid {
				//TODO exit?
			}
		flowLoop:
			for _, flow := range readyFlows {
				for i := 0; i < flow.Priority(); i++ {
					val, ok := flow.Receive()
					if !ok {
						//Channel is closed, remove it from flow list
						delete(d.flows, flow.ID())
						//If last flow, close out chan and exit goroutine
						if len(d.flows) == 0 {
							close(d.outChan)
							return
						}
						continue flowLoop
					} else {
						d.outChan <- val
						if flow.Len() == 0 {
							continue flowLoop
						}
					}
				}
			}
		}
	}()
}
