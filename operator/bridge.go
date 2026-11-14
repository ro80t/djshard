package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Same JSON protocol as src/shared.ts (Msg). One struct covers every message kind.
type msg struct {
	T             string          `json:"t"`
	ID            int             `json:"id,omitempty"`
	Auth          string          `json:"auth,omitempty"`
	Name          string          `json:"name,omitempty"`
	Script        string          `json:"script,omitempty"`
	Context       json.RawMessage `json:"context,omitempty"`
	Cluster       *int            `json:"cluster,omitempty"`
	OK            bool            `json:"ok,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
	Error         string          `json:"error,omitempty"`
	Message       string          `json:"message,omitempty"`
	TotalShards   int             `json:"totalShards,omitempty"`
	TotalClusters int             `json:"totalClusters,omitempty"`
	Clusters      []clusterShards `json:"clusters,omitempty"`
}

type clusterShards struct {
	ID     int   `json:"id"`
	Shards []int `json:"shards"`
}

const evalTimeout = 30 * time.Second

// Mirrors assignment() in src/shared.ts.
func assignment(total, perCluster, machines, slot int) msg {
	n := (total + perCluster - 1) / perCluster
	per := (n + machines - 1) / machines
	clusters := []clusterShards{}
	for id := slot * per; id < min((slot+1)*per, n); id++ {
		shards := []int{}
		for s := id * perCluster; s < min((id+1)*perCluster, total); s++ {
			shards = append(shards, s)
		}
		clusters = append(clusters, clusterShards{id, shards})
	}
	return msg{T: "assign", TotalShards: total, TotalClusters: n, Clusters: clusters}
}

// ordinal returns N for StatefulSet pod "<sts>-N".
func ordinal(sts, pod string) (int, bool) {
	rest, ok := strings.CutPrefix(pod, sts+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	return n, err == nil && n >= 0
}

type conn struct {
	ws      *websocket.Conn
	slot    int
	mu      sync.Mutex
	next    int
	pending map[int]chan msg
}

func (c *conn) request(m msg) (msg, error) {
	c.mu.Lock()
	c.next++
	m.ID = c.next
	ch := make(chan msg, 1)
	c.pending[m.ID] = ch
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), evalTimeout)
	defer cancel()
	if err := wsjson.Write(ctx, c.ws, m); err != nil {
		c.mu.Lock()
		delete(c.pending, m.ID)
		c.mu.Unlock()
		return msg{}, err
	}
	select {
	case r, ok := <-ch:
		if !ok {
			return msg{}, errors.New("machine disconnected")
		}
		if !r.OK {
			return msg{}, errors.New(r.Error)
		}
		return r, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, m.ID)
		c.mu.Unlock()
		return msg{}, errors.New("eval timed out")
	}
}

func (c *conn) deliver(m msg) {
	c.mu.Lock()
	ch := c.pending[m.ID]
	delete(c.pending, m.ID)
	c.mu.Unlock()
	if ch != nil {
		ch <- m
	}
}

func (c *conn) failAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
}

// bridge serves ws://.../{namespace}/{shardcluster}; each pod of a ShardCluster connects once.
// ponytail: all eval traffic goes through this single operator replica; shard per-group if it becomes a bottleneck.
type bridge struct {
	cache  client.Reader // ShardClusters
	direct client.Reader // auth Secrets, read uncached
	mu     sync.Mutex
	groups map[string]map[int]*conn
}

var errUnauthorized = errors.New("unauthorized")

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("ns"), r.PathValue("name")
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 20)
	ctx := r.Context()

	var hello msg
	hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = wsjson.Read(hctx, ws, &hello)
	cancel()
	if err != nil || hello.T != "hello" {
		return
	}
	slot, assign, err := b.authorize(ctx, ns, name, hello)
	if err != nil {
		if errors.Is(err, errUnauthorized) {
			_ = wsjson.Write(ctx, ws, msg{T: "error", Message: err.Error()})
		}
		// Anything else (e.g. shard count not resolved yet) is transient: the manager reconnects.
		ws.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}

	key := ns + "/" + name
	c := &conn{ws: ws, slot: slot, pending: map[int]chan msg{}}
	b.mu.Lock()
	if b.groups[key] == nil {
		b.groups[key] = map[int]*conn{}
	}
	if old := b.groups[key][slot]; old != nil {
		old.ws.CloseNow() // same pod reconnecting; the old socket is dead
	}
	b.groups[key][slot] = c
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if b.groups[key][slot] == c {
			delete(b.groups[key], slot)
		}
		b.mu.Unlock()
		c.failAll()
	}()

	if err := wsjson.Write(ctx, ws, assign); err != nil {
		return
	}
	for {
		var m msg
		if err := wsjson.Read(ctx, ws, &m); err != nil {
			return
		}
		switch m.T {
		case "res":
			c.deliver(m)
		case "eval":
			go func() {
				out := msg{T: "res", ID: m.ID, OK: true}
				data, err := b.broadcast(key, m)
				if err != nil {
					out = msg{T: "res", ID: m.ID, Error: err.Error()}
				}
				out.Data = data
				_ = wsjson.Write(ctx, ws, out)
			}()
		}
	}
}

func (b *bridge) authorize(ctx context.Context, ns, name string, hello msg) (int, msg, error) {
	var sc ShardCluster
	var secret corev1.Secret
	if b.cache.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &sc) != nil ||
		b.direct.Get(ctx, types.NamespacedName{Namespace: ns, Name: authSecretName(name)}, &secret) != nil ||
		subtle.ConstantTimeCompare(secret.Data["auth"], []byte(hello.Auth)) != 1 {
		return 0, msg{}, errUnauthorized
	}
	slot, ok := ordinal(sc.Name, hello.Name)
	if !ok || slot >= int(sc.Spec.Replicas) {
		return 0, msg{}, errors.New("pod is not part of this ShardCluster")
	}
	if sc.Status.TotalShards == 0 {
		return 0, msg{}, errors.New("shard count not resolved yet")
	}
	return slot, assignment(sc.Status.TotalShards, sc.Spec.ShardsPerCluster, int(sc.Spec.Replicas), slot), nil
}

// broadcast runs an eval on every connected pod of the group and concatenates the results in slot order.
func (b *bridge) broadcast(key string, m msg) (json.RawMessage, error) {
	b.mu.Lock()
	var conns []*conn
	for _, c := range b.groups[key] {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	slices.SortFunc(conns, func(a, b *conn) int { return a.slot - b.slot })

	parts := make([][]json.RawMessage, len(conns))
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			r, err := c.request(m)
			if err == nil {
				err = json.Unmarshal(r.Data, &parts[i])
			}
			errs[i] = err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return json.Marshal(slices.Concat(parts...))
}
