// Package records is the records service: CRUD on the records table, an entry
// in record_events for each change (same transaction), and a broadcast of the
// change to every client. All three transports call into it, so they behave
// the same.
package records

import (
	"context"
	"errors"
	"net/netip"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rmx-lt/rmx-backend-probe/internal/apperr"
	"github.com/rmx-lt/rmx-backend-probe/internal/protocol"
)

// ListLimit: the list returns at most this many rows (newest first), so one
// response always fits comfortably in an MQTT message.
const ListLimit = 500

// Source says who makes a change: the transport and, if known, the client's IP.
type Source struct {
	Via protocol.ConnectionType
	// IP is invalid over MQTT (the broker doesn't tell us the client's address).
	IP netip.Addr
}

// ipText is the IP as text for SQL ($n::text::inet), nil if unknown.
func (s Source) ipText() *string {
	if !s.IP.IsValid() {
		return nil
	}
	t := s.IP.String()
	return &t
}

// Service is the records service.
type Service struct {
	pool *pgxpool.Pool
	hub  *Hub
}

// New returns a Service on the pool.
func New(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool, hub: newHub()}
}

// Subscribe receives every change made through this service.
func (s *Service) Subscribe() *Subscription { return s.hub.Subscribe() }

const columns = `id, time, connection_type::text, value, updated_at, host(created_ip)`

func scanRow(row pgx.Row) (protocol.RecordRow, error) {
	var r protocol.RecordRow
	err := row.Scan(&r.ID, &r.Time, &r.ConnectionType, &r.Value, &r.UpdatedAt, &r.CreatedIP)
	r.Time, r.UpdatedAt = r.Time.UTC(), r.UpdatedAt.UTC()
	return r, err
}

// List returns the newest records.
func (s *Service) List(ctx context.Context) ([]protocol.RecordRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+columns+` FROM records ORDER BY time DESC, id DESC LIMIT $1`, ListLimit)
	if err != nil {
		return nil, apperr.DB(err)
	}
	defer rows.Close()
	records := []protocol.RecordRow{}
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, apperr.DB(err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.DB(err)
	}
	return records, nil
}

// Create adds a record.
func (s *Service) Create(ctx context.Context, src Source, in protocol.RecordInput) (protocol.RecordRow, error) {
	if err := in.Validate(); err != nil {
		return protocol.RecordRow{}, apperr.BadRequest("%s", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	defer tx.Rollback(ctx)
	rec, err := scanRow(tx.QueryRow(ctx,
		`INSERT INTO records (connection_type, value, created_ip)
		 VALUES ($1::text::connection_type, $2, $3::text::inet)
		 RETURNING `+columns, string(src.Via), in.Value, src.ipText()))
	if err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	if err := logEvent(ctx, tx, src, protocol.OpCreate, rec.ID, rec.Value); err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	s.hub.publish(protocol.ChangeEvent{V: protocol.EventCreate{Record: rec}})
	return rec, nil
}

// Update changes the value of a record.
func (s *Service) Update(ctx context.Context, src Source, id int64, in protocol.RecordInput) (protocol.RecordRow, error) {
	if err := in.Validate(); err != nil {
		return protocol.RecordRow{}, apperr.BadRequest("%s", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	defer tx.Rollback(ctx)
	rec, err := scanRow(tx.QueryRow(ctx,
		`UPDATE records SET value = $2, connection_type = $3::text::connection_type, updated_at = now()
		 WHERE id = $1
		 RETURNING `+columns, id, in.Value, string(src.Via)))
	if errors.Is(err, pgx.ErrNoRows) {
		return protocol.RecordRow{}, apperr.NotFound(id)
	}
	if err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	if err := logEvent(ctx, tx, src, protocol.OpUpdate, id, rec.Value); err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return protocol.RecordRow{}, apperr.DB(err)
	}
	s.hub.publish(protocol.ChangeEvent{V: protocol.EventUpdate{Record: rec}})
	return rec, nil
}

// Delete removes a record.
func (s *Service) Delete(ctx context.Context, src Source, id int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return apperr.DB(err)
	}
	defer tx.Rollback(ctx)
	var value *string
	err = tx.QueryRow(ctx, `DELETE FROM records WHERE id = $1 RETURNING value`, id).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return apperr.NotFound(id)
	}
	if err != nil {
		return apperr.DB(err)
	}
	if err := logEvent(ctx, tx, src, protocol.OpDelete, id, value); err != nil {
		return apperr.DB(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return apperr.DB(err)
	}
	s.hub.publish(protocol.ChangeEvent{V: protocol.EventDelete{ID: id, ConnectionType: src.Via}})
	return nil
}

// Handle runs one WSS/MQTT request and packs the outcome for the wire.
func (s *Service) Handle(ctx context.Context, src Source, req protocol.Request) protocol.RpcResult {
	var data any
	var err error
	switch r := req.V.(type) {
	case protocol.RequestList:
		var records []protocol.RecordRow
		if records, err = s.List(ctx); err == nil {
			data = protocol.RpcRecords{Records: records}
		}
	case protocol.RequestCreate:
		var rec protocol.RecordRow
		if rec, err = s.Create(ctx, src, protocol.RecordInput(r)); err == nil {
			data = protocol.RpcRecord{Record: rec}
		}
	case protocol.RequestUpdate:
		var rec protocol.RecordRow
		if rec, err = s.Update(ctx, src, r.ID, protocol.RecordInput{Value: r.Value}); err == nil {
			data = protocol.RpcRecord{Record: rec}
		}
	case protocol.RequestDelete:
		if err = s.Delete(ctx, src, r.ID); err == nil {
			data = protocol.RpcDeleted(r)
		}
	default:
		err = apperr.BadRequest("unknown request")
	}
	if err != nil {
		e := apperr.From(err)
		e.Log()
		return protocol.RpcResult{V: protocol.RpcError{Error: e.API()}}
	}
	return protocol.RpcResult{V: protocol.RpcOk{Data: protocol.RpcData{V: data}}}
}

func logEvent(ctx context.Context, tx pgx.Tx, src Source, op protocol.RecordOp, recordID int64, value *string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO record_events (connection_type, op, record_id, value, ip)
		 VALUES ($1::text::connection_type, $2::text::record_op, $3, $4, $5::text::inet)`,
		string(src.Via), string(op), recordID, value, src.ipText())
	return err
}
