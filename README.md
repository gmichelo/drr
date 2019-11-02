# Deficit Round Robin channels scheduler

## Introduction
Sometimes, certain messages are more important than others. The drr package provides a generic implementation of [Deficit Round Robin scheduler](https://en.wikipedia.org/wiki/Deficit_round_robin) for Go's channels. With this package, developer can merge multiple input channels into a single output one by enforcing different input rates. 

## API Documentation
Documentation can be found [here](https://godoc.org/github.com/bigmikes/drr).

## Example
```Go
import (
	"context"
	"fmt"

	"github.com/bigmikes/drr"
)

func sourceRequests(s string) <-chan interface{} {
	inChan := make(chan interface{}, 5)
	go func() {
		defer close(inChan)
		for i := 0; i < 5; i++ {
			inChan <- s
		}
	}()
	return inChan
}

func main() {
	// Set output channel and create DRR scheduler.
	outputChan := make(chan interface{}, 5)
	drr, err := drr.NewDRR(outputChan)
	if err != nil {
		panic(err)
	}

	// Register two input channels with priority 3 and 2 respectively.
	sourceChan1 := sourceRequests("req1")
	drr.Input(3, sourceChan1)
	sourceChan2 := sourceRequests("req2")
	drr.Input(2, sourceChan2)

	// Start DRR
	drr.Start(context.Background())

	// Consume values from output channels.
	// Expected rates are 3/5 for channel with priority 3
	// and 2/5 for channel with priority 2.
	for out := range outputChan {
		fmt.Println(out)
	}
}

// Output:
// req1
// req1
// req1
// req2
// req2
// req1
// req1
// req2
// req2
// req2
```

## License 
The drr package is licensed under the MIT License. Please see the LICENSE file for details.

## Contributing and bug reports
You are welcome to open a new issue [here on GitHub](https://github.com/bigmikes/drr/issues) to provide feedbacks. 