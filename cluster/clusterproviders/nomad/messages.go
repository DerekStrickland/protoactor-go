package nomad

// RegisterMember message used to register a new member in nomad
type RegisterMember struct{}

// DeregisterMember Empty struct used to deregister a member from nomad
type DeregisterMember struct{}

// StartWatchingCluster message used to start watching a nomad cluster
type StartWatchingCluster struct {
	ClusterName string
}
