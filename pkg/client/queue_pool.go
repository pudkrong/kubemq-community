package client

import (
	"context"
	"fmt"
	"github.com/kubemq-io/broker/pkg/nuid"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/cmap"
	"go.uber.org/atomic"
	"sync"
	"time"
)

type QueuePoolClient struct {
	Channel     string
	Client      *QueueClient
	LastConnect time.Time
	usedCounter *atomic.Uint32
}

func NewQueuePoolClient(channel string, opts *Options, policyCfg *config.QueueConfig) (*QueuePoolClient, error) {
	qc, err := NewQueueClient(opts, policyCfg)
	if err != nil {
		return nil, err
	}
	qc.isPoolClient = true
	return &QueuePoolClient{
		Channel:     channel,
		Client:      qc,
		LastConnect: time.Time{},
		usedCounter: atomic.NewUint32(0),
	}, nil
}

func (qpc *QueuePoolClient) GetClient() *QueueClient {
	qpc.usedCounter.Inc()
	return qpc.Client
}

func (qpc *QueuePoolClient) ReleaseClient() {
	qpc.LastConnect = time.Now()
	qpc.usedCounter.Dec()
}

func (qpc *QueuePoolClient) InUsed(d time.Duration) bool {
	if qpc.usedCounter.Load() == 0 && time.Since(qpc.LastConnect) > d {
		return false
	}
	return true
}

type QueuePoolOptions struct {
	KillAfter time.Duration
	MaxUsage  uint32
}
type QueuePool struct {
	opts      *QueuePoolOptions
	poolMap   cmap.ConcurrentMap
	isUp      *atomic.Bool
	appConfig *config.Config
	// mu serializes GetClient's check-and-(re)build against the watcher/Close
	// eviction so a freshly rebuilt client cannot be torn down out from under
	// an in-flight transaction (which would orphan and leak it).
	mu sync.Mutex
}

func NewQueuePool(ctx context.Context, opts *QueuePoolOptions, appConfig *config.Config) *QueuePool {
	qp := &QueuePool{
		opts:      opts,
		poolMap:   cmap.New(),
		isUp:      atomic.NewBool(true),
		appConfig: appConfig,
	}
	go qp.runWatcher(ctx)
	return qp
}
func (qp *QueuePool) getNewClientOpts() *Options {
	return NewClientOptions(fmt.Sprintf("%s-queue-pool_client-%s", qp.appConfig.Host, nuid.Next())).
		SetMemoryPipe(qp.appConfig.Broker.MemoryPipe).
		SetMaxInflight(int(qp.appConfig.Queue.MaxInflight)).
		SetPubAckWaitSeconds(int(qp.appConfig.Queue.PubAckWaitSeconds)).
		SetAutoReconnect(false)
}
func (qp *QueuePool) runWatcher(ctx context.Context) {
	for {
		select {
		case <-time.After(qp.opts.KillAfter):
			qp.mu.Lock()
			var removeList []string
			for key, value := range qp.poolMap.Items() {
				client := value.(*QueuePoolClient)
				if !client.InUsed(qp.opts.KillAfter) {
					_ = client.Client.Disconnect()
					removeList = append(removeList, key)
				}
			}
			for _, key := range removeList {
				qp.poolMap.Remove(key)
			}
			qp.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
func (qp *QueuePool) ClientsCount() int {
	return qp.poolMap.Count()
}
func (qp *QueuePool) Close() {
	qp.isUp.Store(false)
	qp.mu.Lock()
	defer qp.mu.Unlock()
	var removeList []string
	for key, value := range qp.poolMap.Items() {
		client := value.(*QueuePoolClient)
		if !client.InUsed(qp.opts.KillAfter) {
			_ = client.Client.Disconnect()
			removeList = append(removeList, key)
		}
	}
	for _, key := range removeList {
		qp.poolMap.Remove(key)
	}
}
func (qp *QueuePool) GetClient(channel string) (*QueueClient, error) {
	if !qp.isUp.Load() {
		return nil, fmt.Errorf("pool is shutting down, cannot create new clients")
	}
	if channel == "" {
		return nil, fmt.Errorf("bad channel name, cannot be empty")
	}

	qp.mu.Lock()
	defer qp.mu.Unlock()

	value, ok := qp.poolMap.Get(channel)
	if ok {
		client := value.(*QueuePoolClient)
		// Self-heal: if this pooled client's STAN connection died (isUp=false)
		// and nothing is currently using it, replace it with a fresh client
		// before handing it out. Without this, a poisoned client kept being
		// returned for the same channel, so every poll failed (Error 121 /
		// Error 302) until the watcher reaped it up to KillAfter later.
		if !client.Client.isUp.Load() && client.usedCounter.Load() == 0 {
			_ = client.Client.Disconnect()
			qp.poolMap.Remove(channel)
			// fall through and create a fresh client below
		} else {
			return client.GetClient(), nil
		}
	}

	newClient, err := NewQueuePoolClient(channel, qp.getNewClientOpts(), qp.appConfig.Queue)
	if err != nil {
		return nil, err
	}
	qp.poolMap.Set(channel, newClient)

	return newClient.GetClient(), nil
}

func (qp *QueuePool) ReleaseClient(channel string) {
	value, ok := qp.poolMap.Get(channel)
	if ok {
		client := value.(*QueuePoolClient)
		client.ReleaseClient()
	}
}
