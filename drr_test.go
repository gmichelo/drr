package drr

import (
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
		drr = NewDRR(outChan)
		So(drr, ShouldNotEqual, nil)
	})
	Convey("Register flow", t, func() {
		flow1 := generator("flow1", 5)
		flow2 := generator("flow2", 5)
		drr.Input(2, flow1)
		drr.Input(1, flow2)
	})
	Convey("Check output", t, func() {
		drr.Start()
		for out := range drr.Output() {
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
		drr = NewDRR(outChan)
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
		drr.Start()
		outputPayloads := make(map[int][]interface{})
		for out := range drr.Output() {
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
