// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.6.1
// source: proto/server_stream.proto

package custom_proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Path     string    `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	Metadata *Metadata `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_server_stream_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_proto_server_stream_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_proto_server_stream_proto_rawDescGZIP(), []int{0}
}

func (x *Entry) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *Entry) GetMetadata() *Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

type Metadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamResponseTimeNs string `protobuf:"bytes,1,opt,name=stream_response_time_ns,json=streamResponseTimeNs,proto3" json:"stream_response_time_ns,omitempty"`
	SubscriptionId       string `protobuf:"bytes,2,opt,name=subscription_id,json=subscriptionId,proto3" json:"subscription_id,omitempty"`
}

func (x *Metadata) Reset() {
	*x = Metadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_server_stream_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Metadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Metadata) ProtoMessage() {}

func (x *Metadata) ProtoReflect() protoreflect.Message {
	mi := &file_proto_server_stream_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Metadata.ProtoReflect.Descriptor instead.
func (*Metadata) Descriptor() ([]byte, []int) {
	return file_proto_server_stream_proto_rawDescGZIP(), []int{1}
}

func (x *Metadata) GetStreamResponseTimeNs() string {
	if x != nil {
		return x.StreamResponseTimeNs
	}
	return ""
}

func (x *Metadata) GetSubscriptionId() string {
	if x != nil {
		return x.SubscriptionId
	}
	return ""
}

type Updates struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Entry *Entry `protobuf:"bytes,1,opt,name=entry,proto3" json:"entry,omitempty"` // Other fields...
}

func (x *Updates) Reset() {
	*x = Updates{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_server_stream_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Updates) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Updates) ProtoMessage() {}

func (x *Updates) ProtoReflect() protoreflect.Message {
	mi := &file_proto_server_stream_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Updates.ProtoReflect.Descriptor instead.
func (*Updates) Descriptor() ([]byte, []int) {
	return file_proto_server_stream_proto_rawDescGZIP(), []int{2}
}

func (x *Updates) GetEntry() *Entry {
	if x != nil {
		return x.Entry
	}
	return nil
}

type ServerStreamResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Updates *Updates `protobuf:"bytes,1,opt,name=updates,proto3" json:"updates,omitempty"` // Other fields...
}

func (x *ServerStreamResponse) Reset() {
	*x = ServerStreamResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_server_stream_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ServerStreamResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServerStreamResponse) ProtoMessage() {}

func (x *ServerStreamResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_server_stream_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServerStreamResponse.ProtoReflect.Descriptor instead.
func (*ServerStreamResponse) Descriptor() ([]byte, []int) {
	return file_proto_server_stream_proto_rawDescGZIP(), []int{3}
}

func (x *ServerStreamResponse) GetUpdates() *Updates {
	if x != nil {
		return x.Updates
	}
	return nil
}

var File_proto_server_stream_proto protoreflect.FileDescriptor

var file_proto_server_stream_proto_rawDesc = []byte{
	0x0a, 0x19, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x73,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x42, 0x0a, 0x05, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x25, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x4d, 0x65, 0x74,
	0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x22,
	0x6a, 0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x35, 0x0a, 0x17, 0x73,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x5f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x14, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x54, 0x69, 0x6d, 0x65,
	0x4e, 0x73, 0x12, 0x27, 0x0a, 0x0f, 0x73, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x73, 0x75, 0x62,
	0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0x27, 0x0a, 0x07, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x12, 0x1c, 0x0a, 0x05, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x06, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x05, 0x65,
	0x6e, 0x74, 0x72, 0x79, 0x22, 0x3a, 0x0a, 0x14, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x53, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x22, 0x0a, 0x07,
	0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x08, 0x2e,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x52, 0x07, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73,
	0x42, 0x24, 0x5a, 0x22, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62,
	0x6f, 0x6a, 0x61, 0x6e, 0x64, 0x2f, 0x67, 0x68, 0x7a, 0x2f, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d,
	0x5f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_server_stream_proto_rawDescOnce sync.Once
	file_proto_server_stream_proto_rawDescData = file_proto_server_stream_proto_rawDesc
)

func file_proto_server_stream_proto_rawDescGZIP() []byte {
	file_proto_server_stream_proto_rawDescOnce.Do(func() {
		file_proto_server_stream_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_server_stream_proto_rawDescData)
	})
	return file_proto_server_stream_proto_rawDescData
}

var file_proto_server_stream_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_proto_server_stream_proto_goTypes = []interface{}{
	(*Entry)(nil),                // 0: Entry
	(*Metadata)(nil),             // 1: Metadata
	(*Updates)(nil),              // 2: Updates
	(*ServerStreamResponse)(nil), // 3: ServerStreamResponse
}
var file_proto_server_stream_proto_depIdxs = []int32{
	1, // 0: Entry.metadata:type_name -> Metadata
	0, // 1: Updates.entry:type_name -> Entry
	2, // 2: ServerStreamResponse.updates:type_name -> Updates
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_proto_server_stream_proto_init() }
func file_proto_server_stream_proto_init() {
	if File_proto_server_stream_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_server_stream_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_server_stream_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Metadata); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_server_stream_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Updates); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_server_stream_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ServerStreamResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_server_stream_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_server_stream_proto_goTypes,
		DependencyIndexes: file_proto_server_stream_proto_depIdxs,
		MessageInfos:      file_proto_server_stream_proto_msgTypes,
	}.Build()
	File_proto_server_stream_proto = out.File
	file_proto_server_stream_proto_rawDesc = nil
	file_proto_server_stream_proto_goTypes = nil
	file_proto_server_stream_proto_depIdxs = nil
}
