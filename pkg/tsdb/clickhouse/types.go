package clickhouse

type TargetResponseDTO struct {
	MetaData   []meta                   `json:"meta"`
	Rows       int                      `json:"rows"`
	Statistics statistics               `json:"statistics"`
	Data       []map[string]interface{} `json:"data"`
}

type meta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type statistics struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  uint64  `json:"rows_read"`
	BytesRead uint64  `json:"bytes_read"`
}
