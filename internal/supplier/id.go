package supplier

import (
	"fmt"
	"net/http"
	"strconv"
)

const MaxID = 1_000_000

func ParseQuery(r *http.Request) (int, error) {
	v := r.URL.Query().Get("supplier_id")
	if v == "" {
		return 0, fmt.Errorf("supplier_id is required")
	}
	id, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid supplier_id")
	}
	if err := Validate(id); err != nil {
		return 0, err
	}
	return id, nil
}

func Validate(id int) error {
	if id <= 0 || id > MaxID {
		return fmt.Errorf("invalid supplier_id")
	}
	return nil
}
