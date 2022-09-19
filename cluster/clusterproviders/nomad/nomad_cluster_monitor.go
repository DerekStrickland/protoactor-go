package nomad

import (
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"github.com/asynkron/protoactor-go/scheduler"
)

type nomadClusterMonitorActor struct {
	*Provider
	actor.Behavior

	refreshCanceller scheduler.CancelFunc
}

func (ncm *nomadClusterMonitorActor) Receive(ctx actor.Context) { ncm.Behavior.Receive(ctx) }

func (ncm *nomadClusterMonitorActor) init(ctx actor.Context) {
	switch r := ctx.Message().(type) {
	case *RegisterMember:
		// make sure timeout is set to some meaningful value
		timeout := ctx.ReceiveTimeout()
		if timeout.Microseconds() == 0 {
			timeout = ncm.Provider.cluster.Config.RequestTimeoutTime
			if timeout.Microseconds() == 0 {
				timeout = time.Second * 5 // default to 5 seconds
			}
		}

		if err := ncm.registerMember(timeout); err != nil {
			plog.Error("Failed to register service to nomad, will retry", log.Error(err))
			ctx.Send(ctx.Self(), r)
			return
		}
		plog.Info("Registered service to nomad")
	case *DeregisterMember:
		if ncm.watching {
			if err := ncm.deregisterMember(ctx.ReceiveTimeout()); err != nil {
				plog.Error("Failed to deregister service from nomad, will retry", log.Error(err))
				ctx.Send(ctx.Self(), r)
				return
			}
			ncm.shutdown = true
			plog.Info("Deregistered service from nomad")
		}
	case *StartWatchingCluster:
		if err := ncm.startWatchingCluster(ctx.ReceiveTimeout()); err != nil {
			plog.Error("Failed to start watching nomad cluster, will retry", log.Error(err))
			ctx.Send(ctx.Self(), r)
			return
		}
		plog.Info("nomad cluster started to being watched")
	}
}

// creates and initializes a new nomadClusterMonitorActor in the heap and
// returns a reference to its memory address
func newClusterMonitor(provider *Provider) actor.Actor {
	ncm := nomadClusterMonitorActor{
		Behavior: actor.NewBehavior(),
		Provider: provider,
	}
	ncm.Become(ncm.init)
	return &ncm
}
