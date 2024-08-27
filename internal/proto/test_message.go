package proto

type TestMessage struct {
	Foo  string `msg:"foo" json:"foo,omitempty"`
	Hoge string `msg:"hoge" json:"hoge,omitempty"`
}
