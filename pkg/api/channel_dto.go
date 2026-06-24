package api

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"time"
)

type ChannelDTO struct {
	Name              string         `json:"name"`
	Type              string         `json:"type"`
	LastActivity      int64          `json:"lastActivity"`
	LastActivityHuman string         `json:"lastActivityHuman"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
	IsActive          bool           `json:"isActive"`
	ChannelKey        string         `json:"channelKey"`
	Clients           []*ClientDTO   `json:"clients"`
}

// create a new ChannelDTO from Entity

func NewChannelDTO(_type, name string, entity *Entity, clientsEntities *EntitiesFamily) *ChannelDTO {
	c := &ChannelDTO{
		Name:              name,
		Type:              _type,
		Total:             NewBaseValuesDTOFromBaseValues(entity.In.CombineWIth(entity.Out)),
		Incoming:          NewBaseValuesDTOFromBaseValues(entity.In),
		Outgoing:          NewBaseValuesDTOFromBaseValues(entity.Out),
		LastActivity:      entity.LastSeen,
		LastActivityHuman: humanize.Time(time.UnixMilli(entity.LastSeen)),
		IsActive:          entity.IsActive(),
	}
	c.ChannelKey = fmt.Sprintf("%s-%s", c.Type, c.Name)
	for _, client := range clientsEntities.List() {
		if client.Name != "" && client.IsActive() {
			c.Clients = append(c.Clients, NewClientDTO(client.Name, client))
		}
	}
	return c
}
