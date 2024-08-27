package proto

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *TestMessage) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "foo":
			z.Foo, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Foo")
				return
			}
		case "hoge":
			z.Hoge, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Hoge")
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
func (z TestMessage) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "foo"
	err = en.Append(0x82, 0xa3, 0x66, 0x6f, 0x6f)
	if err != nil {
		return
	}
	err = en.WriteString(z.Foo)
	if err != nil {
		err = msgp.WrapError(err, "Foo")
		return
	}
	// write "hoge"
	err = en.Append(0xa4, 0x68, 0x6f, 0x67, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Hoge)
	if err != nil {
		err = msgp.WrapError(err, "Hoge")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z TestMessage) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "foo"
	o = append(o, 0x82, 0xa3, 0x66, 0x6f, 0x6f)
	o = msgp.AppendString(o, z.Foo)
	// string "hoge"
	o = append(o, 0xa4, 0x68, 0x6f, 0x67, 0x65)
	o = msgp.AppendString(o, z.Hoge)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TestMessage) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "foo":
			z.Foo, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Foo")
				return
			}
		case "hoge":
			z.Hoge, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Hoge")
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
func (z TestMessage) Msgsize() (s int) {
	s = 1 + 4 + msgp.StringPrefixSize + len(z.Foo) + 5 + msgp.StringPrefixSize + len(z.Hoge)
	return
}
