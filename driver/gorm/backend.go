package gorm

import (
	"context"
	"fmt"
	"reflect"

	g "github.com/go-mixins/gorm/v4"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/go-mixins/eventsource/driver"
)

type Backend[A comparable] struct {
	*g.Backend

	name string
}

func NewBackend[T any, A comparable](gormBackend *g.Backend) *Backend[A] {
	var t T
	res := &Backend[A]{
		Backend: gormBackend,
		name:    reflect.TypeOf(t).Name(),
	}
	return res
}

func (b *Backend[A]) Connect(migrate bool) error {
	if migrate {
		return b.DB.Scopes(b.scope()).AutoMigrate(&Event[A]{})
	}
	return nil
}

func (b *Backend[A]) WithDebug() *Backend[A] {
	res := *b
	res.Backend = res.Backend.WithDebug()
	return &res
}

func (b *Backend[A]) Codec() driver.Codec {
	return driver.JSON{}
}

type Event[A comparable] struct {
	AggregateID      A   `gorm:"primaryKey;autoIncrement:false"`
	AggregateVersion int `gorm:"primaryKey;autoIncrement:false"`
	Type             string
	Payload          datatypes.JSON
}

func (b *Backend[A]) scope() func(db *gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		tableName := tx.NamingStrategy.TableName(fmt.Sprintf("%s_events", b.name))
		return tx.Table(tableName)
	}
}

func (b *Backend[A]) Load(ctx context.Context, id string, fromVersion, toVersion int) ([]driver.Event[A], error) {
	q := b.WithContext(ctx).DB.Scopes(b.scope())
	var evts []Event[A]
	q = q.Model(Event[A]{}).Where(`aggregate_id = ?`, id)
	if fromVersion != 0 {
		q = q.Where(`aggregate_version >= ?`, fromVersion)
	}
	if toVersion >= 0 {
		q = q.Where(`aggregate_version <= ?`, toVersion)
	}
	if err := q.Find(&evts).Error; err != nil {
		return nil, err
	}
	res := make([]driver.Event[A], len(evts))
	for i, e := range evts {
		res[i] = driver.Event[A]{
			AggregateID:      e.AggregateID,
			AggregateVersion: e.AggregateVersion,
			Type:             e.Type,
			Payload:          e.Payload,
		}
	}
	return res, nil
}

func (b *Backend[A]) Save(ctx context.Context, events []driver.Event[A]) (rErr error) {
	tx := b.WithContext(ctx).Begin()
	defer func() {
		rErr = tx.End(rErr)
	}()
	q := tx.DB.Scopes(b.scope()).Model(&Event[A]{})
	for _, e := range events {
		err := q.Create(&Event[A]{
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
