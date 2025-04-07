package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"

	// "math/rand"
	"time"
	// "fmt"

	"google.golang.org/grpc/metadata"

	// "reflect"

	// "github.com/google/uuid"

	// "github.com/docker/docker/client"

	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// StatsHandler is for gRPC stats
type statsHandler struct {
	results chan *callResult
	id      int
	hasLog  bool
	log     Logger

	lock   sync.RWMutex
	ignore bool
}

type Value struct {
	Timestamp time.Time `json:"timestamp"`
	Float     float64   `json:"float"`
}

// Define the Metadata struct
type Metadata struct {
	StreamResponseTimeNs int64  `json:"streamResponseTimeNs,string"`
	SubscriptionID       string `json:"subscriptionId"`
	Description          string `json:"description"`
	Comment              string `json:"comment"`
}

// Define the Entry struct
type Entry struct {
	Path     string   `json:"path"`
	Value    Value    `json:"value"`
	Metadata Metadata `json:"metadata"`
}

// Define the Update struct
type Update struct {
	Entry  Entry    `json:"entry"`
	Fields []string `json:"fields"`
}

type Field int32

const (
	Field_FIELD_UNSPECIFIED                Field = 0  // "*" i.e. everything
	Field_FIELD_PATH                       Field = 1  // path
	Field_FIELD_VALUE                      Field = 2  // value
	Field_FIELD_ACTUATOR_TARGET            Field = 3  // actuator_target
	Field_FIELD_METADATA                   Field = 10 // metadata.*
	Field_FIELD_METADATA_DATA_TYPE         Field = 11 // metadata.data_type
	Field_FIELD_METADATA_DESCRIPTION       Field = 12 // metadata.description
	Field_FIELD_METADATA_ENTRY_TYPE        Field = 13 // metadata.entry_type
	Field_FIELD_METADATA_COMMENT           Field = 14 // metadata.comment
	Field_FIELD_METADATA_DEPRECATION       Field = 15 // metadata.deprecation
	Field_FIELD_METADATA_UNIT              Field = 16 // metadata.unit
	Field_FIELD_METADATA_VALUE_RESTRICTION Field = 17 // metadata.value_restriction.*
	Field_FIELD_METADATA_ACTUATOR          Field = 20 // metadata.actuator.*
	Field_FIELD_METADATA_SENSOR            Field = 30 // metadata.sensor.*
	Field_FIELD_METADATA_ATTRIBUTE         Field = 40 // metadata.attribute.*
)

// Define the Response struct
type Response struct {
	Updates []Update `json:"updates"`
}

type MutableObject struct {
	InMetadata metadata.MD // Example mutable field
}

// HandleConn handle the connection
func (c *statsHandler) HandleConn(ctx context.Context, cs stats.ConnStats) {

	// no-op
}

// TagConn exists to satisfy gRPC stats.Handler.
func (c *statsHandler) TagConn(ctx context.Context, cti *stats.ConnTagInfo) context.Context {
	// no-op

	return ctx
}

// Helper function to calculate CPU usage percentage
// func calculateCPUPercentage(stat *types.StatsJSON) float64 {
// 	cpuDelta := float64(stat.CPUStats.CPUUsage.TotalUsage) - float64(stat.PreCPUStats.CPUUsage.TotalUsage)
// 	systemDelta := float64(stat.CPUStats.SystemUsage) - float64(stat.PreCPUStats.SystemUsage)
// 	cpuPercent := (cpuDelta / systemDelta) * float64(len(stat.CPUStats.CPUUsage.PercpuUsage)) * 100.0
// 	return cpuPercent
// }

// Helper function to calculate memory usage percentage
// func calculateMemoryPercentage(stat *types.StatsJSON) float64 {
// 	memUsage := float64(stat.MemoryStats.Usage)
// 	memLimit := float64(stat.MemoryStats.Limit)
// 	memPercent := (memUsage / memLimit) * 100.0
// 	return memPercent
// }

// HandleRPC implements per-RPC tracing and stats instrumentation.
func (c *statsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	switch rs := rs.(type) {

	case *stats.InHeader:
		var headerValue metadata.MD
		// You can access the `InHeader` field from the `s` object to get the received headers.
		if rs.Client {
			ign := false
			c.lock.RLock()
			ign = c.ignore
			c.lock.RUnlock()
			if !ign {
				headerValue = rs.Header

				if header, ok := ctx.Value("InHeader").(*MutableObject); ok {
					header.InMetadata = headerValue
				}
			}
		}

	case *stats.InPayload:
		// str, _ := json.Marshal(rs)
		// fmt.Printf("---mm %+v\n", string(str))
		// fmt.Printf("---mm %+v\n", ctx)

		serverStreamResponseByte, _ := json.Marshal(rs.Payload)
		// serverStreamResponseBytes := []byte{serverStreamResponseByte[0]}
		// fmt.Println(string(serverStreamResponseByte))
		serverStreamResponseByteRaw := json.RawMessage(string(serverStreamResponseByte))
		var stream_data Response
		err2 := json.Unmarshal(serverStreamResponseByteRaw, &stream_data)
		// fmt.Printf("%#v", stream_data.Updates[0].Entry.Metadata.Description)
		if err2 != nil {
			fmt.Println("Shut up", err2)
		}
		// log.Printf(stream_data.Updates[0].Entry.Metadata.StreamResponseTimeNs)
		// log.Printf(stream_data.Updates[0].Entry.Metadata.SubscriptionID)
		// log.Printf("%+v", stream_data.Updates[0].Entry.Value.Float)
		// StreamResponseTimeNs_tm, err := strconv.ParseInt(stream_data.Updates[0].Entry.Metadata.StreamResponseTimeNs, 10, 64)
		// if err != nil {
		// 	panic(err)
		// }

		// fmt.Printf("-->%+v<--\n", ctx.Value("call_type"))
		// fmt.Printf("-->%+v<--\n", ctx.Value("metadata"))
		// fmt.Printf("-->%+v<--\n", ctx.Value("set_id"))
		// fmt.Printf("-->%+v<--\n", len(stream_data.Updates))
		// fmt.Printf("-->%+v<--\n", stream_data.Updates[0].Entry.Value.Float)
		if (ctx.Value("call_type") == "kuksa.val.v1.VAL.Subscribe" && len(stream_data.Updates) > 0 && stream_data.Updates[0].Entry.Value != Value{} && stream_data.Updates[0].Entry.Value.Float != 0) {
			StreamResponseTimeNs_tm := time.Unix(int64(math.Abs(float64(stream_data.Updates[0].Entry.Metadata.StreamResponseTimeNs)/1000_000_000)), stream_data.Updates[0].Entry.Metadata.StreamResponseTimeNs%1000_000_000)
			broker_to_client_ts := rs.RecvTime.Sub(StreamResponseTimeNs_tm)
			if rs.Client {
				ign := false
				c.lock.RLock()
				ign = c.ignore
				c.lock.RUnlock()
				if !ign {
					// msg := rs.Payload.(*dynamic.Message).ConvertTo(proto.Message)
					// fmt.Printf("----->%+v\n", rs.Payload)
					c.results <- &callResult{nil, "", ctx.Value("metadata").(string), stream_data.Updates[0].Entry.Metadata.Description, stream_data.Updates[0].Entry.Metadata.SubscriptionID, 0, time.Time{}, time.Time{}, time.Time{}, StreamResponseTimeNs_tm, 0, broker_to_client_ts, 0, 10, 10}

				}
			}
		}

	case *stats.End:

		ign := false
		c.lock.RLock()
		ign = c.ignore
		c.lock.RUnlock()
		if !ign {
			duration := rs.EndTime.Sub(rs.BeginTime)

			var st string
			s, ok := status.FromError(rs.Error)
			if ok {
				st = s.Code().String()
			}

			// Retrieve the header value from the context

			// fmt.Printf("------------->>>>>>>>>%T",ctx.Value("InHeader"))
			// fmt.Println("------------->>>>>>>>>",ctx.Value("InHeader"))
			var databroker_exit_ts time.Time
			var databroker_enter_ts time.Time
			var client_to_broker_ts time.Duration
			var broker_to_client_ts time.Duration
			var request_process_time time.Duration
			if header, ok := ctx.Value("InHeader").(*MutableObject); ok {

				if len(header.InMetadata) > 2 {
					databroker_exit_timestamp, err1 := strconv.ParseInt(header.InMetadata["databroker_exit_ts"][0], 10, 64)
					databroker_enter_timestamp, err2 := strconv.ParseInt(header.InMetadata["databroker_enter_ts"][0], 10, 64)

					// fmt.Println(databroker_enter_timestamp, databroker_exit_timestamp)
					if err1 == nil && err2 == nil {

						databroker_exit_ts = time.Unix(int64(math.Abs(float64(databroker_exit_timestamp)/1000000000)), databroker_exit_timestamp%1000000000)
						databroker_enter_ts = time.Unix(int64(math.Abs(float64(databroker_enter_timestamp)/1000000000)), databroker_enter_timestamp%1000000000)
						broker_to_client_ts = rs.EndTime.Sub(databroker_exit_ts)
						client_to_broker_ts = databroker_enter_ts.Sub(rs.BeginTime)
						request_process_time = databroker_exit_ts.Sub(databroker_enter_ts)

					}
				}

			}

			if ctx.Value("call_type") == "kuksa.val.v1.VAL.Set" {
				c.results <- &callResult{rs.Error, st, ctx.Value("metadata").(string), ctx.Value("set_id").(string), "set_call", duration, rs.EndTime, rs.BeginTime, databroker_enter_ts, databroker_exit_ts, client_to_broker_ts, broker_to_client_ts, request_process_time, 10, 10}
			}

			if c.hasLog {
				c.log.Debugw("Received RPC Stats",
					"statsID", c.id, "code", st, "error", rs.Error,
					"duration", duration, "stats", rs)
			}
		}
	}

}

func (c *statsHandler) Ignore(val bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.ignore = val
}

// TagRPC implements per-RPC context management.
func (c *statsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {

	ctx = context.WithValue(ctx, "InHeader", &MutableObject{})

	return ctx
}
