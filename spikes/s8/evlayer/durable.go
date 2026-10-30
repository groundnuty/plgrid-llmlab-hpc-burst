package evlayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DurableConfig configures a server-side durable pull consumer with explicit acks.
type DurableConfig struct {
	Stream        string
	Durable       string
	AckWait       time.Duration // redelivery after this long without an ack
	MaxAckPending int           // >1 allows later messages to overtake an unacked one
	NakDelay      time.Duration // redelivery delay after a handler error
	Batch         int
	FetchWait     time.Duration
}

// Durable is a subscriber whose cursor lives in the broker (the consumer's ack
// state). It adds, on top of JetStream: an explicit gap marker, and
// per-resource re-sequencing of redeliveries.
type Durable struct {
	cfg     DurableConfig
	stream  jetstream.Stream
	cons    jetstream.Consumer
	gaps    gapTracker
	applied map[string]uint64        // resource key -> highest stream seq handled for it
	floor   uint64                   // ack floor at attach: everything at or below is done
	held    map[uint64]jetstream.Msg // NAKed messages of this session, acknowledged when a later delivery re-sequences them
	// Resequenced counts predecessors the layer fetched ahead of a later delivery.
	Resequenced int
	// Duplicates counts redelivered copies dropped because they were already handled.
	Duplicates int
}

// NewDurable attaches to (or creates) the durable consumer.
func NewDurable(ctx context.Context, js jetstream.JetStream, cfg DurableConfig) (*Durable, error) {
	if cfg.Batch == 0 {
		cfg.Batch = 16
	}
	if cfg.FetchWait == 0 {
		cfg.FetchWait = 300 * time.Millisecond
	}
	if cfg.NakDelay == 0 {
		cfg.NakDelay = 200 * time.Millisecond
	}
	st, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		return nil, err
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPending,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: SubjectsAll,
	})
	if err != nil {
		return nil, err
	}
	ci, err := cons.Info(ctx)
	if err != nil {
		return nil, err
	}
	d := &Durable{cfg: cfg, stream: st, cons: cons, applied: map[string]uint64{}, held: map[uint64]jetstream.Msg{}, floor: ci.AckFloor.Stream}
	d.gaps = gapTracker{stream: cfg.Stream, next: ci.Delivered.Stream + 1}
	if ci.Delivered.Stream == 0 {
		d.gaps.next = 0 // fresh consumer: no history owed
	}
	return d, nil
}

// Run delivers records to h until ctx ends. A gap marker is delivered in
// sequence position, before the first record after the gap.
func (d *Durable) Run(ctx context.Context, h Handler) error {
	if err := d.checkIdle(ctx, h); err != nil {
		return err
	}
	for ctx.Err() == nil {
		batch, err := d.cons.Fetch(d.cfg.Batch, jetstream.FetchMaxWait(d.cfg.FetchWait))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("evlayer: fetch: %w", err)
		}
		got := 0
		for m := range batch.Messages() {
			got++
			if ctx.Err() != nil {
				_ = m.Nak() // shutting down: give the message back now instead of pinning the ack floor for AckWait
				continue
			}
			if err := d.process(ctx, m, h); err != nil {
				if ctx.Err() != nil {
					_ = m.Nak()
					continue
				}
				return err
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if berr := batch.Error(); berr != nil && !errors.Is(berr, nats.ErrTimeout) && ctx.Err() == nil {
			return fmt.Errorf("evlayer: fetch batch: %w", berr)
		}
		if got == 0 {
			if err := d.checkIdle(ctx, h); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Durable) checkIdle(ctx context.Context, h Handler) error {
	si, err := d.stream.Info(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if gap := d.gaps.idle(si.State.FirstSeq); gap != nil {
		return h(ctx, Record{Seq: gap.To, Gap: gap})
	}
	return nil
}

func (d *Durable) process(ctx context.Context, m jetstream.Msg, h Handler) error {
	md, err := m.Metadata()
	if err != nil {
		return err
	}
	seq := md.Sequence.Stream
	if gap := d.gaps.observe(seq); gap != nil {
		if err := h(ctx, Record{Seq: gap.To, Gap: gap}); err != nil {
			return err
		}
	}
	rec, err := decodeRecord(seq, m.Data())
	if err != nil {
		return err // poison message: fail fast, never skip silently
	}
	rec.Redelivered = md.NumDelivered > 1
	last := d.applied[rec.Resource]
	if last < d.floor {
		last = d.floor
	}
	if seq <= last {
		d.Duplicates++
		delete(d.held, seq)
		return m.Ack() // a copy of something already handled
	}
	// Hand over every earlier, unhandled message of the same resource first, read
	// from the stream (the source of truth), not from the delivery order.
	for {
		raw, err := d.stream.GetMsg(ctx, last+1, jetstream.WithGetMsgSubject(m.Subject()))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return fmt.Errorf("evlayer: resequence read: %w", err)
		}
		if raw.Sequence >= seq {
			break
		}
		pre, err := decodeRecord(raw.Sequence, raw.Data)
		if err != nil {
			return err
		}
		pre.Resequenced = true
		if err := h(ctx, pre); err != nil {
			d.held[seq] = m
			return m.NakWithDelay(d.cfg.NakDelay) // the earlier one failed: this one waits behind it
		}
		d.Resequenced++
		d.applied[rec.Resource] = raw.Sequence
		last = raw.Sequence
		if hm, ok := d.held[raw.Sequence]; ok { // we hold its delivery: acknowledge it now, so the ack floor can advance
			_ = hm.Ack()
			delete(d.held, raw.Sequence)
		}
	}
	if err := h(ctx, rec); err != nil {
		d.held[seq] = m
		return m.NakWithDelay(d.cfg.NakDelay)
	}
	d.applied[rec.Resource] = seq
	delete(d.held, seq)
	return m.Ack()
}
