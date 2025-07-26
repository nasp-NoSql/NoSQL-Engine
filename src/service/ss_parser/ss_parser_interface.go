package ss_parser

import (
	"nosqlEngine/src/models/key_value"
)

type SSParser interface {
	FlushMemtable(keyValues []key_value.KeyValue)
}