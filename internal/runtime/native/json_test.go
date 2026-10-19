package native

import "encoding/json"

func jsonMarshal(v any) ([]byte, error) { return json.MarshalIndent(v, "", "  ") }
