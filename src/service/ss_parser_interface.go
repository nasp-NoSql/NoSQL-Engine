package service

import "nosqlEngine/src/models/key_value"

type SSParser interface{
	AddMemtable(keyValues []key_value.KeyValue)
}