package main

import (
	"github.com/martinlindhe/base36"
)

func encode(param string) string {
	return base36.EncodeBytes([]byte(param))
}

func decode(param string) string {
	return string(base36.DecodeToBytes(param))
}
