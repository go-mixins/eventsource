package driver

import "encoding/json"

type JSON struct{}

func (JSON) Unmarshal(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}

func (JSON) Marshal(src any) ([]byte, error) {
	return json.Marshal(src)
}
