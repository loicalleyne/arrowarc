package proto

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *AckResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "ack":
			z.Ack, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Ack")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z AckResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "ack"
	err = en.Append(0x81, 0xa3, 0x61, 0x63, 0x6b)
	if err != nil {
		return
	}
	err = en.WriteString(z.Ack)
	if err != nil {
		err = msgp.WrapError(err, "Ack")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z AckResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "ack"
	o = append(o, 0x81, 0xa3, 0x61, 0x63, 0x6b)
	o = msgp.AppendString(o, z.Ack)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *AckResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "ack":
			z.Ack, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Ack")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z AckResp) Msgsize() (s int) {
	s = 1 + 4 + msgp.StringPrefixSize + len(z.Ack)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Entry) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 2 {
		err = msgp.ArrayError{Wanted: 2, Got: zb0001}
		return
	}
	z.Time, err = dc.ReadInt64()
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, err = dc.ReadIntf()
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Entry) EncodeMsg(en *msgp.Writer) (err error) {
	// array header, size 2
	err = en.Append(0x92)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	err = en.WriteIntf(z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Entry) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// array header, size 2
	o = append(o, 0x92)
	o = msgp.AppendInt64(o, z.Time)
	o, err = msgp.AppendIntf(o, z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Entry) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 2 {
		err = msgp.ArrayError{Wanted: 2, Got: zb0001}
		return
	}
	z.Time, bts, err = msgp.ReadInt64Bytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, bts, err = msgp.ReadIntfBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Entry) Msgsize() (s int) {
	s = 1 + msgp.Int64Size + msgp.GuessSize(z.Record)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Forward) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 3 {
		err = msgp.ArrayError{Wanted: 3, Got: zb0001}
		return
	}
	z.Tag, err = dc.ReadString()
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err, "Entries")
		return
	}
	if cap(z.Entries) >= int(zb0002) {
		z.Entries = (z.Entries)[:zb0002]
	} else {
		z.Entries = make([]Entry, zb0002)
	}
	for za0001 := range z.Entries {
		var zb0003 uint32
		zb0003, err = dc.ReadArrayHeader()
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001)
			return
		}
		if zb0003 != 2 {
			err = msgp.ArrayError{Wanted: 2, Got: zb0003}
			return
		}
		z.Entries[za0001].Time, err = dc.ReadInt64()
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Time")
			return
		}
		z.Entries[za0001].Record, err = dc.ReadIntf()
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Record")
			return
		}
	}
	var zb0004 uint32
	zb0004, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0004)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0004 > 0 {
		zb0004--
		var za0002 string
		var za0003 string
		za0002, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0003, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option", za0002)
			return
		}
		z.Option[za0002] = za0003
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Forward) EncodeMsg(en *msgp.Writer) (err error) {
	// array header, size 3
	err = en.Append(0x93)
	if err != nil {
		return
	}
	err = en.WriteString(z.Tag)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Entries)))
	if err != nil {
		err = msgp.WrapError(err, "Entries")
		return
	}
	for za0001 := range z.Entries {
		// array header, size 2
		err = en.Append(0x92)
		if err != nil {
			return
		}
		err = en.WriteInt64(z.Entries[za0001].Time)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Time")
			return
		}
		err = en.WriteIntf(z.Entries[za0001].Record)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Record")
			return
		}
	}
	err = en.WriteMapHeader(uint32(len(z.Option)))
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	for za0002, za0003 := range z.Option {
		err = en.WriteString(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		err = en.WriteString(za0003)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0002)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Forward) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// array header, size 3
	o = append(o, 0x93)
	o = msgp.AppendString(o, z.Tag)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Entries)))
	for za0001 := range z.Entries {
		// array header, size 2
		o = append(o, 0x92)
		o = msgp.AppendInt64(o, z.Entries[za0001].Time)
		o, err = msgp.AppendIntf(o, z.Entries[za0001].Record)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Record")
			return
		}
	}
	o = msgp.AppendMapHeader(o, uint32(len(z.Option)))
	for za0002, za0003 := range z.Option {
		o = msgp.AppendString(o, za0002)
		o = msgp.AppendString(o, za0003)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Forward) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 3 {
		err = msgp.ArrayError{Wanted: 3, Got: zb0001}
		return
	}
	z.Tag, bts, err = msgp.ReadStringBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Entries")
		return
	}
	if cap(z.Entries) >= int(zb0002) {
		z.Entries = (z.Entries)[:zb0002]
	} else {
		z.Entries = make([]Entry, zb0002)
	}
	for za0001 := range z.Entries {
		var zb0003 uint32
		zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001)
			return
		}
		if zb0003 != 2 {
			err = msgp.ArrayError{Wanted: 2, Got: zb0003}
			return
		}
		z.Entries[za0001].Time, bts, err = msgp.ReadInt64Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Time")
			return
		}
		z.Entries[za0001].Record, bts, err = msgp.ReadIntfBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Entries", za0001, "Record")
			return
		}
	}
	var zb0004 uint32
	zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0004)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0004 > 0 {
		var za0002 string
		var za0003 string
		zb0004--
		za0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0003, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0002)
			return
		}
		z.Option[za0002] = za0003
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Forward) Msgsize() (s int) {
	s = 1 + msgp.StringPrefixSize + len(z.Tag) + msgp.ArrayHeaderSize
	for za0001 := range z.Entries {
		s += 1 + msgp.Int64Size + msgp.GuessSize(z.Entries[za0001].Record)
	}
	s += msgp.MapHeaderSize
	if z.Option != nil {
		for za0002, za0003 := range z.Option {
			_ = za0003
			s += msgp.StringPrefixSize + len(za0002) + msgp.StringPrefixSize + len(za0003)
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Message) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 4 {
		err = msgp.ArrayError{Wanted: 4, Got: zb0001}
		return
	}
	z.Tag, err = dc.ReadString()
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	z.Time, err = dc.ReadInt64()
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, err = dc.ReadIntf()
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	var zb0002 uint32
	zb0002, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0002)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0002 > 0 {
		zb0002--
		var za0001 string
		var za0002 string
		za0001, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0002, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
		z.Option[za0001] = za0002
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Message) EncodeMsg(en *msgp.Writer) (err error) {
	// array header, size 4
	err = en.Append(0x94)
	if err != nil {
		return
	}
	err = en.WriteString(z.Tag)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	err = en.WriteInt64(z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	err = en.WriteIntf(z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Option)))
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	for za0001, za0002 := range z.Option {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		err = en.WriteString(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Message) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// array header, size 4
	o = append(o, 0x94)
	o = msgp.AppendString(o, z.Tag)
	o = msgp.AppendInt64(o, z.Time)
	o, err = msgp.AppendIntf(o, z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	o = msgp.AppendMapHeader(o, uint32(len(z.Option)))
	for za0001, za0002 := range z.Option {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendString(o, za0002)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Message) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 4 {
		err = msgp.ArrayError{Wanted: 4, Got: zb0001}
		return
	}
	z.Tag, bts, err = msgp.ReadStringBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	z.Time, bts, err = msgp.ReadInt64Bytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, bts, err = msgp.ReadIntfBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0002)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0002 > 0 {
		var za0001 string
		var za0002 string
		zb0002--
		za0001, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
		z.Option[za0001] = za0002
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Message) Msgsize() (s int) {
	s = 1 + msgp.StringPrefixSize + len(z.Tag) + msgp.Int64Size + msgp.GuessSize(z.Record) + msgp.MapHeaderSize
	if z.Option != nil {
		for za0001, za0002 := range z.Option {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.StringPrefixSize + len(za0002)
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MessageExt) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 4 {
		err = msgp.ArrayError{Wanted: 4, Got: zb0001}
		return
	}
	z.Tag, err = dc.ReadString()
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	err = dc.ReadExtension(&z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, err = dc.ReadIntf()
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	var zb0002 uint32
	zb0002, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0002)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0002 > 0 {
		zb0002--
		var za0001 string
		var za0002 string
		za0001, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0002, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
		z.Option[za0001] = za0002
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *MessageExt) EncodeMsg(en *msgp.Writer) (err error) {
	// array header, size 4
	err = en.Append(0x94)
	if err != nil {
		return
	}
	err = en.WriteString(z.Tag)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	err = en.WriteExtension(&z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	err = en.WriteIntf(z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Option)))
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	for za0001, za0002 := range z.Option {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		err = en.WriteString(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MessageExt) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// array header, size 4
	o = append(o, 0x94)
	o = msgp.AppendString(o, z.Tag)
	o, err = msgp.AppendExtension(o, &z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	o, err = msgp.AppendIntf(o, z.Record)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	o = msgp.AppendMapHeader(o, uint32(len(z.Option)))
	for za0001, za0002 := range z.Option {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendString(o, za0002)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MessageExt) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 4 {
		err = msgp.ArrayError{Wanted: 4, Got: zb0001}
		return
	}
	z.Tag, bts, err = msgp.ReadStringBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Tag")
		return
	}
	bts, err = msgp.ReadExtensionBytes(bts, &z.Time)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	z.Record, bts, err = msgp.ReadIntfBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Record")
		return
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Option")
		return
	}
	if z.Option == nil {
		z.Option = make(map[string]string, zb0002)
	} else if len(z.Option) > 0 {
		for key := range z.Option {
			delete(z.Option, key)
		}
	}
	for zb0002 > 0 {
		var za0001 string
		var za0002 string
		zb0002--
		za0001, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option")
			return
		}
		za0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Option", za0001)
			return
		}
		z.Option[za0001] = za0002
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *MessageExt) Msgsize() (s int) {
	s = 1 + msgp.StringPrefixSize + len(z.Tag) + msgp.ExtensionPrefixSize + z.Time.Len() + msgp.GuessSize(z.Record) + msgp.MapHeaderSize
	if z.Option != nil {
		for za0001, za0002 := range z.Option {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.StringPrefixSize + len(za0002)
		}
	}
	return
}
