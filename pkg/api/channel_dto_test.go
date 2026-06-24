package api

import (
	"testing"
	"time"
)

func TestNewChannelDTO_ExcludesInactiveClients(t *testing.T) {
	now := time.Now().UTC().UnixMilli()

	active := NewEntity("queue_client", "active-client")
	active.LastSeen = now

	stale := NewEntity("queue_client", "stale-client")
	stale.LastSeen = now - 600000 // 10 minutes ago, outside the 5-minute active window

	clientsFamily := NewEntitiesFamily("queues/mychannel")
	clientsFamily.AddEntity(active)
	clientsFamily.AddEntity(stale)

	channelEntity := NewEntity("queue", "mychannel")
	channelEntity.LastSeen = now

	dto := NewChannelDTO("queues", "mychannel", channelEntity, clientsFamily)

	if len(dto.Clients) != 1 {
		t.Fatalf("expected only the active client in the clients array, got %d", len(dto.Clients))
	}
	if dto.Clients[0].Name != "active-client" {
		t.Errorf("expected the active client to remain, got %q", dto.Clients[0].Name)
	}
	for _, c := range dto.Clients {
		if c.Name == "stale-client" {
			t.Error("stale (inactive) client should have been excluded from the clients array")
		}
	}
}
