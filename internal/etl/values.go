package etl

type EtlStatus string

const (
	StatusNew        EtlStatus = "new"
	StatusProcessing EtlStatus = "processing"
	StatusSent       EtlStatus = "sent"
	StatusExported   EtlStatus = "exported"
)
