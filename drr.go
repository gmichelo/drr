package drr

import (
	"context"
	"log"
	"sync"

	"github.com/bigmikes/drr/flow"
)

type DRR struct {
	sync.Mutex
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
	d.Lock()
	defer d.Unlock()
	oldLen := len(d.flows)
	d.flows[d.lastID] = flow.NewFlow(d.lastID, prio, in)
	d.lastID++
	log.Printf("Registered flow %v\n", d.lastID)
	//First flow got registered, start DRR goroutine
	if oldLen == 0 {
		log.Println("First flow registered, start DRR goroutine")
		d.runDRR()
	}
}

func mapToSlice(mapIn map[int]*flow.Flow) []*flow.Flow {
	outSlice := make([]*flow.Flow, 0, len(mapIn))
	for _, elem := range mapIn {
		outSlice = append(outSlice, elem)
	}
	return outSlice
}

func (d *DRR) runDRR() {
	go func() {
		for {
			d.Lock()
			log.Println("Total flows", len(d.flows))
			readyFlows, valid := flow.GetReadyChannels(
				context.TODO(), mapToSlice(d.flows))
			log.Println("Ready flows", len(readyFlows))
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
			d.Unlock()
		}
	}()
}
