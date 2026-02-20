package actors

import (
	"context"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/messages"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
)

// NotifierActor runs one per node. It subscribes to a Redis Pub/Sub channel
// for new job notifications and forwards them as NewJobNotification messages
// to the DispatcherActor.
type NotifierActor struct {
	rdb           rueidis.Client
	channel       string
	dispatcherPID *actor.PID
	cancel        context.CancelFunc
	engine        *actor.Engine
	pid           *actor.PID
	logger        gochainedlog.Logger
}

// NewNotifierActor returns a Hollywood Producer that creates a NotifierActor.
func NewNotifierActor(rdb rueidis.Client, channel string, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &NotifierActor{
			rdb:     rdb,
			channel: channel,
			logger:  logger,
		}
	}
}

// SetDispatcherPID sets the PID of the DispatcherActor so new job
// notifications can be forwarded to it. Called by the server during wiring.
func (n *NotifierActor) SetDispatcherPID(pid *actor.PID) {
	n.dispatcherPID = pid
}

// Receive implements actor.Receiver.
func (n *NotifierActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case actor.Started:
		n.onStarted(ctx)
	case actor.Stopped:
		n.onStopped()
	case messages.WireDispatcherPIDMsg:
		n.dispatcherPID = msg.PID
		n.logger.Info().String("pid", msg.PID.String()).Msg("notifier: dispatcher PID wired")
	case *messages.NewJobFromRedisMsg:
		n.onNewJob(ctx, msg)
	case messages.NewJobFromRedisMsg:
		n.onNewJob(ctx, &msg)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (n *NotifierActor) onStarted(ctx *actor.Context) {
	n.engine = ctx.Engine()
	n.pid = ctx.PID()

	subCtx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel

	go n.subscribeLoop(subCtx)

	n.logger.Info().String("channel", n.channel).Msg("notifier actor started")
}

func (n *NotifierActor) onStopped() {
	if n.cancel != nil {
		n.cancel()
	}
	n.logger.Info().Msg("notifier actor stopped")
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func (n *NotifierActor) onNewJob(ctx *actor.Context, msg *messages.NewJobFromRedisMsg) {
	if n.dispatcherPID == nil {
		n.logger.Warn().String("job_id", msg.JobID).Msg("notifier: dispatcher PID not set, dropping notification")
		return
	}

	ctx.Send(n.dispatcherPID, &pb.NewJobNotification{
		JobId:    msg.JobID,
		TaskType: msg.TaskType,
		Priority: int32(msg.Priority),
	})
}

// ---------------------------------------------------------------------------
// Redis Pub/Sub subscriber
// ---------------------------------------------------------------------------

func (n *NotifierActor) subscribeLoop(ctx context.Context) {
	subscribeCmd := n.rdb.B().Subscribe().Channel(n.channel).Build()
	err := n.rdb.Receive(ctx, subscribeCmd, func(pubSubMsg rueidis.PubSubMessage) {
		var msg messages.NewJobFromRedisMsg
		if err := sonic.ConfigFastest.Unmarshal([]byte(pubSubMsg.Message), &msg); err != nil {
			n.logger.Warn().String("payload", pubSubMsg.Message).Err(err).Msg("notifier: failed to unmarshal job notification")
			return
		}
		n.engine.Send(n.pid, &msg)
	})
	if err != nil && ctx.Err() == nil {
		n.logger.Warn().Err(err).Msg("notifier: subscribe loop ended with error")
	}
}
