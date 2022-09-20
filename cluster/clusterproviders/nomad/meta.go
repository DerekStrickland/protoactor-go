package nomad

// Meta keys that will be used to update the allocs metadata
const (
	MetaPrefix      = "cluster.proto.actor/"
	MetaPort        = MetaPrefix + "port"
	MetaKind        = MetaPrefix + "kind"
	MetaCluster     = MetaPrefix + "cluster"
	MetaStatusValue = MetaPrefix + "status-value"
	MetaMemberID    = MetaPrefix + "member-id"
)
