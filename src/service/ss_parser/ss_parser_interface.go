package ss_parser

import "nosqlEngine/src/models/key_value"

type SSParser interface{
	AddMemtable(keyValues []key_value.KeyValue)
}