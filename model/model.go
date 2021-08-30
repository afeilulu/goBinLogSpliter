package model

type BinLog struct {
	Timestamp      int64
	EventType      string
	Rows           interface{}
	Query          string
	SchemaName     string
	TableName      string
	BinlogFileNum  int64
	BinlogPosition int64
	Gtid           string
	Pri            interface{}
	EventID        int64
	ColumnMapping  interface{}
}
