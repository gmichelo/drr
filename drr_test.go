package drr

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	chanSize = 10
)

func generator(prefix string, n int) chan interface{} {
	out := make(chan interface{}, chanSize)
	go func() {
		for i := 0; i < n; i++ {
			out <- fmt.Sprintf("%v: %d", prefix, i)
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
		for {
			select {
			case out, open := <-drr.Output():
				if open {
					s, ok := out.(string)
					So(ok, ShouldEqual, true)
					So(s, ShouldNotEqual, "")
					fmt.Println(s)
				} else {
					return
				}
			}
		}
	})
}
