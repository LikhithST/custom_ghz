package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	v1 "github.com/bojand/ghz/cmd/ghz/kuksa/val/v1"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

// TickValue is the tick value
type TickValue struct {
	instant   time.Time
	reqNumber uint64
}

// Worker is used for doing a single stream of requests in parallel
type Worker struct {
	stub grpcdynamic.Stub
	mtd  *desc.MethodDescriptor

	config   *RunConfig
	workerID string
	active   bool
	stopCh   chan bool
	ticks    <-chan TickValue

	dataProvider     DataProviderFunc
	metadataProvider MetadataProviderFunc
	msgProvider      StreamMessageProviderFunc

	streamRecv StreamRecvMsgInterceptFunc
}

type VectorClock struct {
	Timestamp   string         `json:"timestamp"`
	ProcessName string         `json:"process_name"`
	Vector      map[string]int `json:"vector"`
}

func (w *Worker) runWorker() error {
	var err error
	g := new(errgroup.Group)

	for {
		select {
		case <-w.stopCh:
			if w.config.async {
				return g.Wait()
			}

			return err
		case tv := <-w.ticks:
			if w.config.async {
				g.Go(func() error {
					return w.makeRequest(tv)
				})
			} else {
				rErr := w.makeRequest(tv)
				err = multierr.Append(err, rErr)
			}
		}
	}
}

// Stop stops the worker. It has to be started with Run() again.
func (w *Worker) Stop() {
	if !w.active {
		return
	}

	w.active = false
	w.stopCh <- true
}

func (w *Worker) makeRequest(tv TickValue) error {
	reqNum := int64(tv.reqNumber)

	ctd := newCallData(w.mtd, w.workerID, reqNum, !w.config.disableTemplateFuncs, !w.config.disableTemplateData, w.config.funcs)

	reqMD, err := w.metadataProvider(ctd)
	if err != nil {
		return err
	}

	if w.config.enableCompression {
		reqMD.Append("grpc-accept-encoding", gzip.Name)
	}

	ctx := context.Background()
	var cancel context.CancelFunc

	if w.config.timeout > 0 {
		var metadata *map[string]string

		// fmt.Printf("-----%#v", string(w.config.data))

		_ = json.Unmarshal(w.config.metadata, &metadata)
		ctx = context.WithValue(ctx, "call_type", w.config.call)
		ctx = context.WithValue(ctx, "metadata", (*metadata)["request_id"])
		// ctx = context.WithValue(ctx, "set_id", data.Updates[0].Entry.Metadata.Description)
		ctx, cancel = context.WithTimeout(ctx, w.config.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// include the metadata
	if reqMD != nil {
		ctx = metadata.NewOutgoingContext(ctx, *reqMD)
	}

	inputs, err := w.dataProvider(ctd)
	if err != nil {
		return err
	}

	var msgProvider StreamMessageProviderFunc
	if w.msgProvider != nil {
		msgProvider = w.msgProvider
	} else if w.mtd.IsClientStreaming() {
		if w.config.streamDynamicMessages {
			mp, err := newDynamicMessageProvider(w.mtd, w.config.data, w.config.streamCallCount, !w.config.disableTemplateFuncs, !w.config.disableTemplateData)
			if err != nil {
				return err
			}

			msgProvider = mp.GetStreamMessage
		} else {
			mp, err := newStaticMessageProvider(w.config.streamCallCount, inputs)
			if err != nil {
				return err
			}

			msgProvider = mp.GetStreamMessage
		}
	}

	if len(inputs) == 0 && msgProvider == nil {
		return fmt.Errorf("no data provided for request")
	}

	var callType string
	if w.config.hasLog {
		callType = "unary"
		if w.mtd.IsClientStreaming() && w.mtd.IsServerStreaming() {
			callType = "bidi"
		} else if w.mtd.IsServerStreaming() {
			callType = "server-streaming"
		} else if w.mtd.IsClientStreaming() {
			callType = "client-streaming"
		}

		w.config.log.Debugw("Making request", "workerID", w.workerID,
			"call type", callType, "call", w.mtd.GetFullyQualifiedName(),
			"input", inputs, "metadata", reqMD)
	}

	// RPC errors are handled via stats handler
	if w.mtd.IsClientStreaming() && w.mtd.IsServerStreaming() {
		// fmt.Println("-----------1--------------")
		_ = w.makeBidiRequest(&ctx, ctd, msgProvider)
	} else if w.mtd.IsClientStreaming() {
		// fmt.Println("-----------2--------------")
		_ = w.makeClientStreamingRequest(&ctx, ctd, msgProvider)
	} else if w.mtd.IsServerStreaming() {
		// fmt.Println("-----------3--------------")
		_ = w.makeServerStreamingRequest(&ctx, inputs[0])
	} else {
		// fmt.Println("-----------4--------------")
		_ = w.makeUnaryRequest(&ctx, reqMD, inputs[0])
	}

	return err
}

// func write_to_file() {
// 	fileName := "output.txt"
// 	content := "Hello, this is a test content written to a file using Go!"

// 	// Create or open the file
// 	file, err := os.Create(fileName)
// 	if err != nil {
// 		fmt.Println("Error creating file:", err)
// 		return
// 	}
// 	defer func() {
// 		if err := file.Close(); err != nil {
// 			fmt.Println("Error closing file:", err)
// 		}
// 	}()

// 	// Write content to the file
// 	_, err = file.WriteString(content)
// 	if err != nil {
// 		fmt.Println("Error writing to file:", err)
// 		return
// 	}

// 	fmt.Println("File written successfully!")
// }

// func readLastLine(fileName string) (string, error) {
// 	file, err := os.Open(fileName)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()

// 	var lastLine string
// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		lastLine = scanner.Text()
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return "", err
// 	}

// 	return lastLine, nil
// }

func readLastVectorClock(fileName string) (VectorClock, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return VectorClock{}, err
	}
	defer file.Close()

	var lastLine string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return VectorClock{}, err
	}

	var vectorClock VectorClock
	if err := json.Unmarshal([]byte(lastLine), &vectorClock); err != nil {
		return VectorClock{}, err
	}

	return vectorClock, nil
}

func appendVectorClock(fileName string, vectorClock VectorClock) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Apply an exclusive lock to prevent concurrent writes
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN) // Unlock when done

	jsonData, err := json.Marshal(vectorClock)
	if err != nil {
		return err
	}

	_, err = file.WriteString(string(jsonData) + "\n")
	return err
}

func metadataToMap(md *metadata.MD) map[string][]string {
	metadataMap := make(map[string][]string)
	if md == nil {
		return metadataMap
	}

	for key, values := range *md {
		metadataMap[key] = values
	}

	return metadataMap
}

// mergeVectorClocks compares two maps and retains the maximum values for each key.
func mergeVectorClocks(vc1, vc2 map[string]int) map[string]int {
	merged := make(map[string]int)

	// Iterate over first map and store values
	for k, v := range vc1 {
		merged[k] = v
	}

	// Iterate over second map, keeping the maximum value for each key
	for k, v := range vc2 {
		if existingVal, found := merged[k]; !found || v > existingVal {
			merged[k] = v
		}
	}

	return merged
}

// func updateVectorClockInMetadata(reqMD map[string]string, vectorClock map[string]int) {
// 	// Convert vector clock to a string (you could use JSON or other formats as needed)
// 	// For simplicity, we'll use a comma-separated key-value format here.
// 	vectorClockStr := ""
// 	for node, timestamp := range vectorClock {
// 		vectorClockStr += fmt.Sprintf("%s:%d,", node, timestamp)
// 	}

// 	// Remove trailing comma
// 	if len(vectorClockStr) > 0 {
// 		vectorClockStr = vectorClockStr[:len(vectorClockStr)-1]
// 	}

// 	// Add vector clock string to request metadata (reqMD)
// 	reqMD["vector_clock"] = vectorClockStr
// }

// // Function to set fields dynamically
// func setFields(dynMsg *dynamicpb.Message, data map[string]interface{}) error {
// 	for key, value := range data {
// 		fieldDesc := dynMsg.Descriptor().Fields().ByName(protoreflect.Name(key))
// 		if fieldDesc == nil {
// 			continue
// 		}
// 		dynMsg.Set(fieldDesc, protoreflect.ValueOf(value))
// 	}
// 	return nil
// }

// // Dummy function to simulate loading a FileDescriptorSet
// func getProtoDescriptorSet() []byte {
// 	// Normally, this reads from a `.protoset` file
// 	return []byte{} // Replace with actual descriptor binary
// }

func (w *Worker) makeUnaryRequest(ctx *context.Context, reqMD *metadata.MD, input *dynamic.Message) error {
	var res proto.Message
	var resErr error
	var callOptions = []grpc.CallOption{}
	var header, trailer metadata.MD
	callOptions = append(callOptions, grpc.Header(&header))
	callOptions = append(callOptions, grpc.Trailer(&trailer))
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}
	var data Response
	neww_input := &v1.SetRequest{}
	reqMD_map := metadataToMap(reqMD)
	fileName := "vector_clock_publisher.txt"
	processName := strings.Split(uuid.New().String(), "-")[0] + "_publisher_" + reqMD_map["request_id"][0] //reqMD_map["request_id"][0]
	err := input.ConvertTo(neww_input)
	if err != nil {
		return err
	}
	payload_dataByte, _ := protojson.Marshal(neww_input)
	// payload_dataRaw := json.RawMessage(string(payload_dataByte))
	_ = json.Unmarshal([]byte(string(payload_dataByte)), &data)

	// fmt.Printf("data--- %+v\n", neww_input.Updates[0].Entry.Metadata.GetDescription())
	// fmt.Printf("payload_dataByte--- %+v\n", string(payload_dataByte))
	// msg := dynamicpb.NewMessage(input.GetMessageDescriptor().UnwrapMessage())
	// fmt.Printf("----_---%v\n", msg)
	// fmt.Printf("%v\n", reqMD_map["request_id"][0])
	// println(reqMD_map["request_id"][0])

	if len(data.Updates) > 0 {

		*ctx = context.WithValue(*ctx, "set_id", neww_input.Updates[0].Entry.Metadata.GetDescription())
	}

	// write_to_file()
	// read_txt, _ := readLastLine("output.txt")
	// println(read_txt)

	lastVector, err := readLastVectorClock(fileName)
	if err != nil {
		lastVector = VectorClock{
			Timestamp:   time.Now().Format(time.RFC3339),
			ProcessName: processName,
			Vector:      make(map[string]int),
		}
	}
	// Update vector clock
	lastVector.Timestamp = time.Now().Format(time.RFC3339)
	lastVector.ProcessName = processName
	lastVector.Vector[processName]++

	if err := appendVectorClock(fileName, lastVector); err != nil {
		fmt.Println("Error writing to file:", err)
		return resErr
	}

	lastVector_str, _ := json.Marshal(lastVector)
	lastVector_ptr := string(lastVector_str)
	// fmt.Println("---input---", input, "-------") // Now it's used
	// fmt.Println("---data---", data, "-------")   // Now it's used
	neww_input.Updates[0].Entry.Metadata.Description = &lastVector_ptr
	// data.Updates[0].Entry.Metadata.Description = "18"
	// res_data, err := json.Marshal(data)
	// if err != nil {
	// 	return err
	// }

	// new_input :=

	// 	fmt.Println("Updated vector clockk:", lastVector)
	// fmt.Printf("--%+v\n", input)
	// fmt.Printf("-data-%+v\n", data)
	// start

	// end
	// create a proto.Message to hold the result
	// new_input := &v1.SetRequest{}
	// fmt.Printf("new_input: %+v\n", new_input.Updates)

	// Map to struct
	// marshal dynamic.Message to proto
	// data.Updates[0].Entry.Value.Timestamp = timestamppb.Now()
	// data_byte, err := json.Marshal(data)
	// if err != nil {
	// 	return err
	// }

	// data_raw_byte := json.RawMessage(data_byte)
	// fmt.Println("------", input, "-------")             // Now it's used
	// fmt.Println("------", data_byte, "-------")         // Now it's used
	// fmt.Println("------", string(data_byte), "-------") // Now it's used

	// if err := json.Unmarshal(data_raw_byte, new_input); err != nil {
	// 	fmt.Printf("\n new_input %v", new_input)
	// 	log.Fatalf("Unmarshal error: %v", err)
	// }

	// fmt.Println("------", string(data_byte), "-------") // Now it's used
	// fmt.Println("------", new_input, "-------")         // Now it's used
	// fmt.Println("------", input, "-------")             // Now it's used
	// err = proto.Unmarshal(data_byte, new_input)
	// if err != nil {
	// 	println("error line 387", err)
	// 	return nil
	// }

	// fmt.Println(new_input)
	// println("done")
	// a, _ := json.Marshal(new_input)
	// fmt.Printf(string(a))
	// r := json.RawMessage(string(a))
	// _ = json.Unmarshal(r, &data)

	res, resErr = w.stub.InvokeRpc(*ctx, w.mtd, neww_input, callOptions...)

	if w.config.hasLog {
		inputData, _ := input.MarshalJSON()
		resData, _ := json.Marshal(res)

		w.config.log.Debugw("Received response", "workerID", w.workerID, "call type", "unary",
			"call", w.mtd.GetFullyQualifiedName(),
			"input", string(inputData), "metadata", reqMD,
			"response", string(resData), "error", resErr)
	}

	return resErr
}

func (w *Worker) makeClientStreamingRequest(ctx *context.Context,
	ctd *CallData, messageProvider StreamMessageProviderFunc) error {
	var str *grpcdynamic.ClientStream
	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}
	str, err := w.stub.InvokeRpcClientStream(*ctx, w.mtd, callOptions...)
	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Client Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "client-streaming",
				"call", w.mtd.GetFullyQualifiedName(), "error", err)
		}

		return err
	}

	closeStream := func() {
		res, closeErr := str.CloseAndReceive()

		if w.config.hasLog {
			w.config.log.Debugw("Close and receive", "workerID", w.workerID, "call type", "client-streaming",
				"call", w.mtd.GetFullyQualifiedName(),
				"response", res, "error", closeErr)
		}
	}

	performSend := func(payload *dynamic.Message) (bool, error) {
		err := str.SendMsg(payload)

		if w.config.hasLog {
			w.config.log.Debugw("Send message", "workerID", w.workerID, "call type", "client-streaming",
				"call", w.mtd.GetFullyQualifiedName(),
				"payload", payload, "error", err)
		}

		if err == io.EOF {
			return true, nil
		}

		return false, err
	}

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	done := false
	counter := uint(0)
	end := false
	for !done && len(cancel) == 0 {
		// default message provider checks counter
		// but we also need to keep our own counts
		// in case of custom client providers

		var payload *dynamic.Message
		payload, err = messageProvider(ctd)

		isLast := false
		if errors.Is(err, ErrLastMessage) {
			isLast = true
			err = nil
		}

		if err != nil {
			if errors.Is(err, ErrEndStream) {
				err = nil
			}
			break
		}

		end, err = performSend(payload)
		if end || err != nil || isLast || len(cancel) > 0 {
			break
		}

		counter++

		if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
			break
		}

		if w.config.streamInterval > 0 {
			wait := time.NewTimer(w.config.streamInterval)
			select {
			case <-wait.C:
				break
			case <-cancel:
				if !wait.Stop() {
					<-wait.C
				}
				done = true
				break
			}
		}
	}

	for len(cancel) > 0 {
		<-cancel
	}

	closeStream()

	close(doneCh)
	close(cancel)

	return nil
}

func (w *Worker) makeServerStreamingRequest(ctx *context.Context, input *dynamic.Message) error {
	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	callCtx, callCancel := context.WithCancel(*ctx)
	defer callCancel()

	str, err := w.stub.InvokeRpcServerStream(callCtx, w.mtd, input, callOptions...)

	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "server-streaming",
				"call", w.mtd.GetFullyQualifiedName(),
				"input", input, "error", err)
		}

		return err
	}

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	interceptCanceled := false
	counter := uint(0)
	for err == nil {
		// we should check before receiving a message too
		if w.config.streamCallDuration > 0 && len(cancel) > 0 {
			<-cancel
			callCancel()
			break
		}

		var res proto.Message

		res, err = str.RecvMsg()
		if res != nil {
			// fmt.Printf("%+v", res)
			func(res proto.Message) {
				res_byte, err := json.Marshal(res)
				if err != nil {
					fmt.Println("error:", err)
					return
				}

				// res_data := &v1.SubscribeResponse{}
				var res_data Response
				var recieved_vc VectorClock
				res_rawdatabyte := json.RawMessage(res_byte)
				_ = json.Unmarshal(res_rawdatabyte, &res_data)
				if len(res_data.Updates) > 0 {
					processName := res_data.Updates[0].Entry.Metadata.SubscriptionID
					fileName := "vector_clock_" + processName + "_subscriber.txt"

					jsonStr := res_data.Updates[0].Entry.Metadata.Description
					err := json.Unmarshal([]byte(jsonStr), &recieved_vc)
					if err != nil {
						fmt.Println("error:", err)
						return
					}
					// fmt.Printf("----%+v\n", res_data)
					lastVector, err := readLastVectorClock(fileName)
					if err != nil {
						lastVector = VectorClock{
							Timestamp:   time.Now().Format(time.RFC3339),
							ProcessName: processName,
							Vector:      make(map[string]int),
						}
					}
					// Update vector clock
					lastVector.Timestamp = time.Now().Format(time.RFC3339)
					lastVector.ProcessName = processName
					lastVector.Vector[processName]++

					lastVector.Vector = mergeVectorClocks(lastVector.Vector, recieved_vc.Vector)

					if err := appendVectorClock(fileName, lastVector); err != nil {
						fmt.Println("Error writing to file:", err)
						return
					}
				}
			}(res)
		}

		// fmt.Printf("%v", res)
		if w.config.hasLog {
			w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "server-streaming",
				"call", w.mtd.GetFullyQualifiedName(),
				"response", res, "error", err)
		}

		// with any of the cancellation operations we can't just bail
		// we have to drain the messages until the server gets the cancel and ends their side of the stream

		if w.streamRecv != nil {
			if converted, ok := res.(*dynamic.Message); ok {
				err = w.streamRecv(converted, err)
				if errors.Is(err, ErrEndStream) && !interceptCanceled {
					interceptCanceled = true
					err = nil

					callCancel()
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}

			break
		}

		counter++

		if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
			callCancel()
		}

		if w.config.streamCallDuration > 0 && len(cancel) > 0 {
			<-cancel
			callCancel()
		}
	}

	close(doneCh)
	close(cancel)

	return err
}

func (w *Worker) makeBidiRequest(ctx *context.Context,
	ctd *CallData, messageProvider StreamMessageProviderFunc) error {

	var callOptions = []grpc.CallOption{}

	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}
	str, err := w.stub.InvokeRpcBidiStream(*ctx, w.mtd, callOptions...)

	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Bidi RPC call error: "+err.Error(),
				"workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.GetFullyQualifiedName(), "error", err)
		}

		return err
	}

	counter := uint(0)
	indexCounter := 0
	recvDone := make(chan bool)
	sendDone := make(chan bool)

	closeStream := func() {
		closeErr := str.CloseSend()

		if w.config.hasLog {
			w.config.log.Debugw("Close send", "workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.GetFullyQualifiedName(), "error", closeErr)
		}
	}

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	var recvErr error

	go func() {
		interceptCanceled := false

		for recvErr == nil {
			var res proto.Message
			res, recvErr = str.RecvMsg()

			if w.config.hasLog {
				w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "bidi",
					"call", w.mtd.GetFullyQualifiedName(),
					"response", res, "error", recvErr)
			}

			if w.streamRecv != nil {
				if converted, ok := res.(*dynamic.Message); ok {
					iErr := w.streamRecv(converted, recvErr)
					if errors.Is(iErr, ErrEndStream) && !interceptCanceled {
						interceptCanceled = true
						if len(cancel) == 0 {
							cancel <- struct{}{}
						}
						recvErr = nil
					}
				}
			}

			if recvErr != nil {
				close(recvDone)
				break
			}
		}
	}()

	go func() {
		done := false

		for err == nil && !done {

			// check at start before send too
			if len(cancel) > 0 {
				<-cancel
				closeStream()
				break
			}

			// default message provider checks counter
			// but we also need to keep our own counts
			// in case of custom client providers

			var payload *dynamic.Message
			payload, err = messageProvider(ctd)

			isLast := false
			if errors.Is(err, ErrLastMessage) {
				isLast = true
				err = nil
			}

			if err != nil {
				if errors.Is(err, ErrEndStream) {
					err = nil
				}

				closeStream()
				break
			}

			err = str.SendMsg(payload)
			if err != nil {
				if err == io.EOF {
					err = nil
				}

				break
			}

			if w.config.hasLog {
				w.config.log.Debugw("Send message", "workerID", w.workerID, "call type", "bidi",
					"call", w.mtd.GetFullyQualifiedName(),
					"payload", payload, "error", err)
			}

			if isLast {
				closeStream()
				break
			}

			counter++
			indexCounter++

			if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
				closeStream()
				break
			}

			if len(cancel) > 0 {
				<-cancel
				closeStream()
				break
			}

			if w.config.streamInterval > 0 {
				wait := time.NewTimer(w.config.streamInterval)
				select {
				case <-wait.C:
					break
				case <-cancel:
					if !wait.Stop() {
						<-wait.C
					}
					closeStream()
					done = true
					break
				}
			}
		}

		close(sendDone)
	}()

	_, _ = <-recvDone, <-sendDone

	for len(cancel) > 0 {
		<-cancel
	}

	close(doneCh)
	close(cancel)

	if err == nil && recvErr != nil {
		err = recvErr
	}

	return err
}
