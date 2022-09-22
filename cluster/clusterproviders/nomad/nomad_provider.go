package nomad

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/log"
	"github.com/google/uuid"
	models "github.com/hashicorp/nomad-openapi/clients/go/v1"
	v1 "github.com/hashicorp/nomad-openapi/v1"
	"k8s.io/apimachinery/pkg/watch"
)

var (
	watchTimeoutSeconds       int64 = 30
	ProviderShuttingDownError       = fmt.Errorf("nomad cluster provider is being shut down")
)

// Convenience type to store meta tags
type MetaTags map[string]string

// This data structure implements a Nomad cluster provider for Proto.Actor
type Provider struct {
	ctx            actor.Context
	id             string
	cluster        *cluster.Cluster
	clusterName    string
	nodeID         string
	jobID          string
	allocID        string
	host           string
	port           int
	knownKinds     []string
	allocations    map[string]*models.Allocation
	clusterMonitor *actor.PID
	deregistered   bool
	shutdown       bool
	watching       bool
	client         *v1.Client
	clientConfig   *v1.ClientConfig
	lastIndex      uint64
}

// make sure our Provider complies with the ClusterProvider interface
var _ cluster.ClusterProvider = (*Provider)(nil)

// New crates a new nomad Provider in the heap and return back a reference to its memory address
func New(opts ...Option) (*Provider, error) {
	cfg := &v1.ClientConfig{}
	return NewWithConfig(cfg, opts...)
}

// NewWithConfig creates a new nomad Provider in the heap using the given configuration
// and options, it returns a reference to its memory address or an error
func NewWithConfig(cfg *v1.ClientConfig, opts ...Option) (*Provider, error) {
	client, err := v1.NewClient(v1.ClientOptsCombined(cfg)...)
	if err != nil {
		return nil, err
	}

	p := Provider{
		client: client,
	}

	// process given options
	for _, opt := range opts {
		opt(&p)
	}
	return &p, nil
}

// initializes the cluster provider
func (p *Provider) init(c *cluster.Cluster) error {
	host, port, err := c.ActorSystem.GetHostPort()
	if err != nil {
		return err
	}

	p.cluster = c
	p.id = strings.Replace(uuid.New().String(), "-", "", -1)
	p.knownKinds = c.GetClusterKinds()
	p.clusterName = c.Config.Name
	p.host = host
	p.port = port

	return nil
}

// StartMember registers the member in the cluster and start it
func (p *Provider) StartMember(c *cluster.Cluster) error {
	if err := p.init(c); err != nil {
		return err
	}

	if err := p.startClusterMonitor(c); err != nil {
		return err
	}

	p.registerMemberAsync(c)
	p.startWatchingClusterAsync(c)

	return nil
}

// StartClient starts the nomad client and monitor watch
func (p *Provider) StartClient(c *cluster.Cluster) error {
	if err := p.init(c); err != nil {
		return err
	}

	if err := p.startClusterMonitor(c); err != nil {
		return err
	}

	p.startWatchingClusterAsync(c)
	return nil
}

func (p *Provider) Shutdown(graceful bool) error {
	if p.shutdown {
		// we are already shut down or shutting down
		return nil
	}

	p.shutdown = true
	if p.clusterMonitor != nil {
		if err := p.cluster.ActorSystem.Root.StopFuture(p.clusterMonitor).Wait(); err != nil {
			plog.Error("Failed to stop nomad-provider actor", log.Error(err))
		}
		p.clusterMonitor = nil
	}
	return nil
}

// starts the cluster monitor in its own goroutine
func (p *Provider) startClusterMonitor(c *cluster.Cluster) error {
	var err error
	p.clusterMonitor, err = c.ActorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newClusterMonitor(p)
	}), "nomad-cluster-monitor")

	if err != nil {
		plog.Error("Failed to start nomad-cluster-monitor actor", log.Error(err))
		return err
	}

	// Requires operators to set node_id in the jobspec meta using node attritbute variable interpolation
	// job "nomad-actor" {
	//   meta {
	//     node_id = "${node.unique.id}"
	//   }
	// }
	p.nodeID = os.Getenv("NOMAD_META_node_id")
	p.allocID = os.Getenv("NOMAD_ALLOC_ID")
	return nil
}

// registers itself as a member asynchronously using an actor
func (p *Provider) registerMemberAsync(c *cluster.Cluster) {
	msg := RegisterMember{}
	c.ActorSystem.Root.Send(p.clusterMonitor, &msg)
}

// registers itself as a member in nomad cluster
func (p *Provider) registerMember(timeout time.Duration) error {
	plog.Info(fmt.Sprintf("Registering nomad provider %s on %s", p.allocID, p.nodeID))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	alloc, queryMeta, err := p.client.Allocations().GetAllocation(ctx, p.allocID)
	if err != nil {
		return fmt.Errorf("unable to get own alloc information for %s: %w", p.allocID, err)
	}

	p.lastIndex = queryMeta.LastIndex

	// TODO: Validate this port
	plog.Info(fmt.Sprintf("Using nomad namespace: %s\nUsing nomad port: %d", alloc.Namespace, p.port))

	metaTags := MetaTags{
		MetaCluster:  p.clusterName,
		MetaPort:     fmt.Sprintf("%d", p.port),
		MetaMemberID: p.id,
	}

	// TODO: Determine what the known kinds should be.
	// add known kinds to meta
	for _, kind := range p.knownKinds {
		metaKey := fmt.Sprintf("%s-%s", MetaKind, kind)
		metaTags[metaKey] = "true"
	}

	job, queryMeta, err := p.client.Jobs().GetJob(ctx, alloc.Job.ID)
	if err != nil {
		return fmt.Errorf("unable to get own job information for %s: %w", alloc.Job.ID, err)
	}
	// add existing meta back
	for key, value := range *job.Meta {
		metaTags[key] = value
	}

	p.jobID = job.ID
	job.SetMeta(metaTags)

	return p.updateJobMeta(ctx, job)
}

func (p *Provider) startWatchingClusterAsync(c *cluster.Cluster) {
	msg := StartWatchingCluster{p.clusterName}
	c.ActorSystem.Root.Send(p.clusterMonitor, &msg)
}

func (p *Provider) startWatchingCluster(timeout time.Duration) error {
	selector := fmt.Sprintf("%s=%s", MetaCluster, p.clusterName)
	if p.watching {
		plog.Info(fmt.Sprintf("Allocs for %s are being watched already", selector))
	}

	plog.Debug(fmt.Sprintf("Starting to watch allocs with %s", selector), log.String("selector", selector))

	// error placeholder
	var watcherr error

	// TODO: Refactor to event stream
	// start a new goroutine to monitor the cluster events
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

	topics:
		map[string]string{
			v1.TopicNode:       p.nodeID,
			v1.TopicJob:        p.jobID,
			v1.TopicAllocation: p.allocID,
		}

		eventStream, queryMeta, err := p.client.Events().Stream(ctx, p.lastIndex, p.clientConfig.Namespace, topics)
		if err != nil {
			herr = fmt.Errorf("unable to watch the cluster status: %w", err)
			return
		}
		p.lastIndex = queryMeta.LastIndex

		for !p.shutdown {

			event, ok := <-eventStream.ResultChan()
			if !ok {
				plog.Error("watcher result channel closed abruptly")
				break
			}

			pod, ok := event.Object.(*v1.Pod)
			if !ok {
				err := fmt.Errorf("could not cast %#v[%T] into v1.Pod", event.Object, event.Object)
				plog.Error(err.Error(), log.Error(err))
				continue
			}

			podClusterName, ok := pod.ObjectMeta.Labels[MetaCluster]
			if !ok {
				plog.Info(fmt.Sprintf("The pod %s is not a Proto.Cluster node", pod.ObjectMeta.Name))
			}

			if podClusterName != p.clusterName {
				plog.Info(fmt.Sprintf("The pod %s is from another cluster %s", pod.ObjectMeta.Name, pod.ObjectMeta.Namespace))
			}

			switch event.Type {
			case watch.Deleted:
				delete(p.clusterPods, pod.UID)
			case watch.Error:
				err := apierrors.FromObject(event.Object)
				plog.Error(err.Error(), log.Error(err))
			default:
				p.clusterPods[pod.UID] = pod
			}

			members := make([]*cluster.Member, 0, len(p.clusterPods))
			for _, clusterPod := range p.clusterPods {
				if clusterPod.Status.Phase == "Running" && len(clusterPod.Status.PodIPs) > 0 {

					var kinds []string
					for key, value := range clusterPod.ObjectMeta.Labels {
						if strings.HasPrefix(key, MetaKind) && value == "true" {
							kinds = append(kinds, strings.Replace(key, fmt.Sprintf("%s-", MetaKind), "", 1))
						}
					}

					host := pod.Status.PodIP
					port, err := strconv.Atoi(pod.ObjectMeta.Labels[MetaPort])
					if err != nil {
						err = fmt.Errorf("can not convert pod meta %s into integer: %w", MetaPort, err)
						plog.Error(err.Error(), log.Error(err))
						continue
					}

					mid := pod.ObjectMeta.Labels[MetaMemberID]
					alive := true
					for _, status := range pod.Status.ContainerStatuses {
						if !status.Ready {
							alive = false
							break
						}
					}

					if !alive {
						continue
					}

					members = append(members, &cluster.Member{
						Id:    mid,
						Host:  host,
						Port:  int32(port),
						Kinds: kinds,
					})
				}
			}

			plog.Debug(fmt.Sprintf("Topology received from Kubernetes %#v", members))
			p.cluster.MemberList.UpdateClusterTopology(members)
		}
	}()

	return watcherr
}

// deregister itself as a member from a nomad cluster
func (p *Provider) deregisterMember(timeout time.Duration) error {
	plog.Info(fmt.Sprintf("Deregistering service %s from %s", p.allocID, p.nodeID))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	alloc, queryMeta, err := p.client.Allocations().GetAllocation(ctx, p.allocID)
	if err != nil {
		return fmt.Errorf("unable to get own alloc information for %s: %w", p.allocID, err)
	}

	p.lastIndex = queryMeta.LastIndex

	job := alloc.GetJob()
	metaTags := *job.Meta

	delete(metaTags, MetaCluster)
	for _, kind := range p.knownKinds {
		delete(metaTags, fmt.Sprintf("%s-%s", MetaKind, kind))
	}

	job.SetMeta(metaTags)

	return p.updateJobMeta(ctx, &job)
}

// prepares an inplace update and sends it to nomad to update job metadata
func (p *Provider) updateJobMeta(ctx context.Context, job *models.Job) error {
	_, writeMeta, err := p.client.Jobs().Post(ctx, job)
	if err != nil {
		return err
	}

	p.lastIndex = writeMeta.LastIndex

	return nil
}
