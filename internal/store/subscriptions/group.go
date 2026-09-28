// Package subscriptions manages coalesced wake-up channels. Callers serialize
// access with their store's subscription mutex.
package subscriptions

import "container/list"

// Group keeps subscribers in registration order and rotates task wake-ups fairly.
// Its zero value is ready to use.
type Group struct {
	order list.List
	byCh  map[chan struct{}]*list.Element
	next  *list.Element
}

func (g *Group) Add() chan struct{} {
	ch := make(chan struct{}, 1)
	if g.byCh == nil {
		g.byCh = make(map[chan struct{}]*list.Element)
	}
	e := g.order.PushBack(ch)
	g.byCh[ch] = e
	if g.next == nil {
		g.next = e
	}
	return ch
}

// Remove closes a subscription after discarding any pending hint. It is idempotent.
func (g *Group) Remove(ch chan struct{}) {
	e, ok := g.byCh[ch]
	if !ok {
		return
	}
	next := e.Next()
	g.order.Remove(e)
	delete(g.byCh, ch)
	if g.next == e {
		if next == nil {
			next = g.order.Front()
		}
		g.next = next
	}
	select {
	case <-ch:
	default:
	}
	close(ch)
}

func (g *Group) Len() int { return g.order.Len() }

// WakeOne delivers one hint, skipping subscribers with a hint already buffered.
func (g *Group) WakeOne() {
	for range g.order.Len() {
		e := g.next
		g.next = e.Next()
		if g.next == nil {
			g.next = g.order.Front()
		}
		select {
		case e.Value.(chan struct{}) <- struct{}{}:
			return
		default:
		}
	}
}

// WakeAll broadcasts a hint for run closure and listener reconnection.
func (g *Group) WakeAll() {
	for ch := range g.byCh {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (g *Group) Close() {
	for ch := range g.byCh {
		g.Remove(ch)
	}
}
