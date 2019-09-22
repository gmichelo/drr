package drr

import (
	"context"
	"log"

	"github.com/bigmikes/drr/flow"
)

type DRR struct {
	flows   map[int]*flow.Flow
	lastID  int
	outChan chan interface{}
}

func NewDRR(outChan chan interface{}) *DRR {
	return &DRR{
		flows:   make(map[int]*flow.Flow),
		outChan: outChan,
	}
}

func (d *DRR) Output() chan interface{} {
	return d.outChan
}

func (d *DRR) Input(prio int, in chan interface{}) {
	log.Printf("Registered flow %v\n", d.lastID)
	d.flows[d.lastID] = flow.NewFlow(d.lastID, prio, in)
	d.lastID++
}

func mapToSlice(mapIn map[int]*flow.Flow) []*flow.Flow {
	outSlice := make([]*flow.Flow, 0, len(mapIn))
	for _, elem := range mapIn {
		outSlice = append(outSlice, elem)
	}
	return outSlice
}

func (d *DRR) Start() {
	go func() {
		for {
			//TODO Review context generation and propagation
			readyFlows, valid := flow.GetReadyChannels(
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
						log.Printf("Flow %v got closed\n", flow.ID())
						//If last flow, close out chan and exit goroutine
						if len(d.flows) == 0 {
							log.Println("All flows got closed, close output chan and exit")
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
