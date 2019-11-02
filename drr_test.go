package drr

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	chanSize = 10
)

func generator(prefix string, n int) chan interface{} {
	payload := make([]interface{}, n)
	for i := 0; i < n; i++ {
		payload[i] = fmt.Sprintf("%s: %d", prefix, i)
	}
	return generatorWithPayload(payload)
}

func generatorWithPayload(payload []interface{}) chan interface{} {
	out := make(chan interface{}, chanSize)
	go func() {
		for _, msg := range payload {
			out <- msg
		}
		close(out)
	}()
	return out
}

func TestDRR(t *testing.T) {
	var drr *DRR
	outChan := make(chan interface{}, 10)
	Convey("Create DRR", t, func() {
		drr, _ = NewDRR(outChan)
		So(drr, ShouldNotEqual, nil)
	})
	Convey("Register flow", t, func() {
		flow1 := generator("flow1", 5)
		flow2 := generator("flow2", 5)
		drr.Input(2, flow1)
		drr.Input(1, flow2)
	})
	Convey("Check output", t, func() {
		drr.Start(context.TODO())
		for out := range outChan {
			s, ok := out.(string)
			So(ok, ShouldEqual, true)
			So(s, ShouldNotEqual, "")
		}
	})
}

func TestIntegrityAndOrder(t *testing.T) {
	nFlows := 100
	flowSize := 100
	var drr *DRR
	outChan := make(chan interface{}, 10)
	Convey("Create DRR", t, func() {
		drr, _ = NewDRR(outChan)
		So(drr, ShouldNotEqual, nil)
	})
	var flows []chan interface{}
	payloads := make(map[int][]interface{})
	Convey("Prepare flow with known payload", t, func() {
		for flowID := 0; flowID < nFlows; flowID++ {
			payload := make([]interface{}, 0, flowSize)
			for x := 0; x < flowSize; x++ {
				msg := fmt.Sprintf("%d:%d", flowID, x)
				payload = append(payload, msg)
			}
			payloads[flowID] = payload
			flows = append(flows, generatorWithPayload(payload))
		}
	})
	Convey("Register all flows", t, func() {
		for prio, f := range flows {
			drr.Input(prio+1, f)
		}
	})
	Convey("Check output w.r.t. known payloads", t, func() {
		drr.Start(context.TODO())
		outputPayloads := make(map[int][]interface{})
		for out := range outChan {
			s, ok := out.(string)
			So(ok, ShouldEqual, true)
			So(s, ShouldNotEqual, "")
			flowID := getFlowID(s)
			outputPayloads[flowID] = append(outputPayloads[flowID], s)
		}

		So(len(outputPayloads), ShouldEqual, len(payloads))
		for flowID, payload := range payloads {
			outPayload := outputPayloads[flowID]
			for i, val := range payload {
				out := outPayload[i]
				if val != out {
					t.Fatalf("for flow %d wanted %v instead of %v", flowID, val, out)
				}
			}
		}
	})
}

func getFlowID(s string) int {
	idStr := strings.Split(s, ":")[0]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(fmt.Errorf("convert of string %s failed: %v", s, err))
	}
	return id
}

func TestMeasureOutputRate(t *testing.T) {
	nFlows := 100
	flowSize := 10000
	outChan := make(chan interface{}, flowSize)
	drr, _ := NewDRR(outChan)
	var flows []chan interface{}
	Convey("Prepare flow with known payload", t, func() {
		for flowID := 0; flowID < nFlows; flowID++ {
			inChan := make(chan interface{}, flowSize)
			for x := 0; x < flowSize; x++ {
				inChan <- flowID
			}
			flows = append(flows, inChan)
		}
	})
	expectedRates := make(map[int]float64)
	totalPrio := float64(0)
	Convey("Register all flows", t, func() {
		for flowID, f := range flows {
			prio := flowID + 1
			drr.Input(prio, f)
			expectedRates[flowID] = float64(prio)
			totalPrio += float64(prio)
		}
		for flowID := range expectedRates {
			expectedRates[flowID] /= totalPrio
		}
	})
	Convey("Check output w.r.t. known payloads", t, func() {
		drr.Start(context.TODO())
		hist := make(map[int]int)
		for i := 0; i < flowSize; i++ {
			val := <-outChan
			flowID := val.(int)
			hist[flowID]++
		}

		for flowID := range hist {
			outputRates := float64(hist[flowID]) / float64(flowSize)
			So(outputRates, ShouldAlmostEqual, expectedRates[flowID], .01)
		}
	})
}

func TestErrorInput(t *testing.T) {
	Convey("Create DRR by passing nil output chan", t, func() {
		drr, err := NewDRR(nil)
		So(drr, ShouldEqual, nil)
		So(err, ShouldEqual, ChannelIsNilError)
	})
	Convey("Create DRR and pass wrong values in Input API", t, func() {
		drr, _ := NewDRR(make(chan interface{}))
		err := drr.Input(0, make(chan interface{}))
		So(err, ShouldEqual, InvalidPriorityValueError)
		err = drr.Input(1, nil)
		So(err, ShouldEqual, ChannelIsNilError)
	})
	Convey("Create DRR and pass wrong values in Input API", t, func() {
		drr, _ := NewDRR(make(chan interface{}))
		err := drr.Start(nil)
		So(err, ShouldEqual, ContextIsNilError)
	})
}

func TestContextExipre(t *testing.T) {
	Convey("Create an empty DRR, start it and cancel the context", t, func() {
		outChan := make(chan interface{})
		drr, _ := NewDRR(outChan)
		ctx, cancel := context.WithCancel(context.Background())
		err := drr.Start(ctx)
		So(err, ShouldEqual, nil)
		cancel()
		val, ok := <-outChan
		So(val, ShouldEqual, nil)
		So(ok, ShouldEqual, false)
	})

	Convey("Create DRR with one flow, start it and cancel the context", t, func() {
		outChan := make(chan interface{})
		drr, _ := NewDRR(outChan)
		flow := generator("flow", 5)
		drr.Input(10, flow)
		ctx, cancel := context.WithCancel(context.Background())
		err := drr.Start(ctx)
		So(err, ShouldEqual, nil)
		val, ok := <-outChan
		So(val, ShouldNotEqual, nil)
		So(ok, ShouldEqual, true)
		cancel()
		val, ok = <-outChan
		So(val, ShouldNotEqual, nil)
		So(ok, ShouldEqual, true)
		val, ok = <-outChan
		So(val, ShouldEqual, nil)
		So(ok, ShouldEqual, false)
	})
}

func BenchmarkOverheadUnloaded(b *testing.B) {
	outChan := make(chan interface{})
	inChan := make(chan interface{})
	drr, _ := NewDRR(outChan)
	drr.Input(10, inChan)
	drr.Start(context.TODO())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inChan <- 5
		<-outChan
	}
}

func ExampleDRR() {
	chanSize := 5
	outChan := make(chan interface{}, chanSize)
	// Create new DRR
	drr, _ := NewDRR(outChan)

	// First input channel with priority = 3
	inChan1 := make(chan interface{}, chanSize)
	prio1 := 3
	// Prepare known workload
	for i := 0; i < chanSize; i++ {
		inChan1 <- "chan1"
	}
	// Register channel into DRR
	drr.Input(prio1, inChan1)

	// Second input channel with priority = 2
	inChan2 := make(chan interface{}, chanSize)
	prio2 := 2
	// Prepare known workload
	for i := 0; i < chanSize; i++ {
		inChan2 <- "chan2"
	}
	// Register channel into DRR
	drr.Input(prio2, inChan2)

	// Start DRR scheduler goroutine
	drr.Start(context.Background())

	// Check the output: over 5 output values
	// 3/5 of them should come from first channel
	// with priority 3 and 2/5 should come from second
	// channel with priority 2.
	for i := 0; i < chanSize; i++ {
		val := <-outChan
		str := val.(string)
		fmt.Println(str)
	}

	// Output:
	// chan1
	// chan1
	// chan1
	// chan2
	// chan2
}
