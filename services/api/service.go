package api

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/labstack/echo/v4"
	"sync"
	"time"
)

const saveInterval = time.Second * 5

type service struct {
	sync.Mutex
	appConfig               *config.Config
	broker                  *broker.Service
	metricsExporter         *metrics.Exporter
	lastSnapshot            *api.Snapshot
	db                      *api.DB
	logger                  *logging.Logger
	lastLoadedEntitiesGroup *api.EntitiesGroup
}

func newService(appConfig *config.Config, broker *broker.Service, exp *metrics.Exporter) *service {
	s := &service{
		appConfig:       appConfig,
		broker:          broker,
		metricsExporter: exp,
		db:              api.NewDB(),
	}
	return s
}

func (s *service) init(ctx context.Context, logger *logging.Logger) error {
	s.logger = logger
	if err := s.db.Init(s.appConfig.Store.StorePath); err != nil {
		return fmt.Errorf("error initializing api db: %s", err.Error())
	}
	var err error
	s.lastLoadedEntitiesGroup, err = s.db.GetLastEntities()
	if err != nil {
		s.logger.Errorf("error getting last entities data from local db: %s", err.Error())
		s.lastLoadedEntitiesGroup = api.NewEntitiesGroup()
	}
	go s.run(ctx)
	return nil
}
func (s *service) stop() error {
	return s.db.Close()
}
func (s *service) run(ctx context.Context) {
	s.logger.Infof("starting api snapshot service")
	go func() {
		ticker := time.NewTicker(saveInterval)
		for {
			select {
			case <-ticker.C:
				s.saveEntitiesGroup(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

}
func (s *service) saveEntitiesGroup(ctx context.Context) {
	currentSnapshot, err := s.getCurrentSnapshot(ctx)
	if err != nil {
		s.logger.Errorf("error getting snapshot: %s", err.Error())
		return
	}
	if err := s.db.SaveEntitiesGroup(currentSnapshot.Entities); err != nil {
		s.logger.Errorf("error saving entities group: %s", err.Error())
		return
	}
	if err := s.db.SaveLastEntitiesGroup(currentSnapshot.Entities); err != nil {
		s.logger.Errorf("error saving last entities group: %s", err.Error())
		return
	}

}

func (s *service) getCurrentSnapshot(ctx context.Context) (*api.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	ss, err := s.metricsExporter.Snapshot()
	if err != nil {
		return nil, err
	}
	ss.Entities = s.lastLoadedEntitiesGroup.Clone().Merge(ss.Entities)
	q, err := s.broker.GetQueues(ctx)
	if err != nil {
		return nil, err
	}
	for _, queue := range q.Queues {
		en, ok := ss.Entities.GetEntity("queues", queue.Name)
		if ok {
			en.Out.Waiting = queue.Waiting
		}
	}
	if s.lastSnapshot == nil {
		ss.System.SetCPUUtilization(0, 0)
	} else {
		ss.System.SetCPUUtilization(s.lastSnapshot.System.Uptime, s.lastSnapshot.System.TotalCPUSeconds)
	}
	s.lastSnapshot = ss
	return ss, nil
}

func (s *service) getSnapshot(c echo.Context) error {
	res := NewResponse(c)
	if s.lastSnapshot == nil {
		_, err := s.getCurrentSnapshot(c.Request().Context())
		if err != nil {
			return res.SetError(err).Send()
		}
	}
	groupDTO := api.NewGroupDTO(s.lastSnapshot.System, s.lastSnapshot.Entities)
	return res.SetResponseBody(groupDTO).Send()
}
