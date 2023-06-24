package gorm

import (
	"context"
	"fmt"
	"reflect"

	g "github.com/go-mixins/gorm/v3"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/go-mixins/eventsource/driver"
)

type Backend struct {
	*g.Backend

	name string
}

func NewBackend[T any](driver gorm.Dialector) *Backend {
	var t T
	gormBackend := g.Backend{Driver: driver}
	res := &Backend{
		Backend: &gormBackend,
		name:    reflect.TypeOf(t).Name(),
	}
	gormBackend.InitSchema = func(d *gorm.DB) error {
		return d.Scopes(res.scope()).AutoMigrate(&Event{})
	}
	return res
}

func (b *Backend) Connect(migrate bool) error {
	b.Migrate = migrate
	return b.Backend.Connect()
}

func (b *Backend) Codec() driver.Codec {
	return driver.JSON{}
}

type Event struct {
	AggregateID      string `gorm:"primaryKey"`
	AggregateVersion int    `gorm:"primaryKey;autoIncrement:false"`
	Type             string
	Payload          datatypes.JSON
}

func (b *Backend) scope() func(db *gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		tableName := tx.NamingStrategy.TableName(fmt.Sprintf("%s_events", b.name))
		return tx.Table(tableName)
	}
}

func (b *Backend) Load(ctx context.Context, id string) ([]driver.Event, error) {
	q := b.WithContext(ctx).DB.Scopes(b.scope())
	var evts []Event
	if err := q.Model(Event{}).Where(`aggregate_id = ?`, id).Find(&evts).Error; err != nil {
		return nil, err
	}
	res := make([]driver.Event, len(evts))
	for i, e := range evts {
		res[i] = driver.Event{
			AggregateID:      e.AggregateID,
			AggregateVersion: e.AggregateVersion,
			Type:             e.Type,
			Payload:          e.Payload,
		}
	}
	return res, nil
}

func (b *Backend) Save(ctx context.Context, events []driver.Event) (rErr error) {
	tx := b.WithContext(ctx).Begin()
	defer func() {
		rErr = tx.End(rErr)
	}()
	q := tx.DB.Scopes(b.scope()).Model(&Event{})
	for _, e := range events {
		err := q.Create(&Event{
			AggregateID:      e.AggregateID,
			AggregateVersion: e.AggregateVersion,
			Type:             e.Type,
			Payload:          e.Payload,
		}).Error
		if g.UniqueViolation(err) {
			return driver.ErrConcurrency
		} else if err != nil {
			return fmt.Errorf("saving event to database: %+v", err)
		}
	}
	return nil
}
