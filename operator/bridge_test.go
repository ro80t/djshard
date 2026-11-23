package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAssignment(t *testing.T) { // same cases as test/djshard.test.ts
	cases := []struct {
		total, per, machines, slot int
		want                       []clusterShards
	}{
		{10, 4, 2, 0, []clusterShards{{0, []int{0, 1, 2, 3}}, {1, []int{4, 5, 6, 7}}}},
		{10, 4, 2, 1, []clusterShards{{2, []int{8, 9}}}},
		{10, 4, 5, 4, []clusterShards{}},
	}
	for _, c := range cases {
		if got := assignment(c.total, c.per, c.machines, c.slot).Clusters; !reflect.DeepEqual(got, c.want) {
			t.Errorf("assignment%v = %v", c, got)
		}
	}
	if n, ok := ordinal("bot", "bot-12"); !ok || n != 12 {
		t.Error("ordinal")
	}
	if _, ok := ordinal("bot", "other-1"); ok {
		t.Error("ordinal accepted foreign pod")
	}
}

func TestBridge(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = addToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&ShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "bot", Namespace: "ns"},
			Spec:   ShardClusterSpec{Replicas: 2, ShardsPerCluster: 2},
			Status: ShardClusterStatus{TotalShards: 8}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "bot-djshard", Namespace: "ns"}, Data: map[string][]byte{"auth": []byte("s3cret")}},
	).Build()
	mux := http.NewServeMux()
	mux.Handle("GET /{ns}/{name}", &bridge{cache: c, direct: c, grace: time.Minute, groups: map[string]*group{}})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ns/bot"
	ctx := context.Background()

	// A fake ClusterManager: answers every eval with its cluster ids.
	dial := func(pod, auth string) (*websocket.Conn, msg) {
		ws, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = wsjson.Write(ctx, ws, msg{T: "hello", Name: pod, Auth: auth})
		var a msg
		_ = wsjson.Read(ctx, ws, &a)
		return ws, a
	}
	if _, m := dial("bot-0", "wrong"); m.T != "error" {
		t.Fatalf("bad auth got %+v", m)
	}
	serve := func(ws *websocket.Conn, a msg) chan msg {
		results := make(chan msg, 1)
		go func() {
			for {
				var m msg
				if wsjson.Read(ctx, ws, &m) != nil {
					return
				}
				if m.T == "res" {
					results <- m
					continue
				}
				ids := []int{}
				for _, c := range a.Clusters {
					ids = append(ids, c.ID)
				}
				data, _ := json.Marshal(ids)
				_ = wsjson.Write(ctx, ws, msg{T: "res", ID: m.ID, OK: true, Data: data})
			}
		}()
		return results
	}
	ws0, a0 := dial("bot-0", "s3cret")
	ws1, a1 := dial("bot-1", "s3cret")
	if len(a0.Clusters) != 2 || a1.Clusters[0].ID != 2 || a1.TotalShards != 8 {
		t.Fatalf("assign %+v %+v", a0, a1)
	}
	serve(ws0, a0)
	res := serve(ws1, a1)
	_ = wsjson.Write(ctx, ws1, msg{T: "eval", ID: 7, Script: "c => c"})
	if r := <-res; r.ID != 7 || !r.OK || string(r.Data) != "[0,1,2,3]" {
		t.Fatalf("eval result %+v %s", r, r.Data)
	}
}

// fakePod is a ClusterManager stand-in that answers every eval with the cluster ids it owns.
type fakePod struct {
	ws      *websocket.Conn
	mu      sync.Mutex
	ids     []int
	assigns chan []int
	results chan msg
}

func TestFailover(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = addToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&ShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "bot", Namespace: "ns"},
			Spec:   ShardClusterSpec{Replicas: 4, ShardsPerCluster: 1},
			Status: ShardClusterStatus{TotalShards: 8}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "bot-djshard", Namespace: "ns"}, Data: map[string][]byte{"auth": []byte("s3cret")}},
	).Build()
	mux := http.NewServeMux()
	mux.Handle("GET /{ns}/{name}", &bridge{cache: c, direct: c, grace: 100 * time.Millisecond, groups: map[string]*group{}})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ns/bot"
	ctx := context.Background()

	connect := func(name string) *fakePod {
		ws, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = wsjson.Write(ctx, ws, msg{T: "hello", Name: name, Auth: "s3cret"})
		p := &fakePod{ws: ws, assigns: make(chan []int, 8), results: make(chan msg, 1)}
		go func() {
			for {
				var m msg
				if wsjson.Read(ctx, ws, &m) != nil {
					return
				}
				switch m.T {
				case "assign":
					ids := []int{}
					for _, c := range m.Clusters {
						ids = append(ids, c.ID)
					}
					p.mu.Lock()
					p.ids = ids
					p.mu.Unlock()
					p.assigns <- ids
				case "res":
					p.results <- m
				case "eval":
					p.mu.Lock()
					data, _ := json.Marshal(p.ids)
					p.mu.Unlock()
					_ = wsjson.Write(ctx, ws, msg{T: "res", ID: m.ID, OK: true, Data: data})
				}
			}
		}()
		return p
	}
	expect := func(p *fakePod, want ...int) {
		t.Helper()
		select {
		case got := <-p.assigns:
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("assign = %v, want %v", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no assign, want %v", want)
		}
	}

	pods := make([]*fakePod, 4)
	for i := range pods {
		pods[i] = connect("bot-" + string(rune('0'+i)))
		expect(pods[i], 2*i, 2*i+1)
	}

	// Pods 0 and 1 die; after the grace period pods 2 and 3 take one orphan slot each.
	pods[0].ws.CloseNow()
	pods[1].ws.CloseNow()
	expect(pods[2], 0, 1, 4, 5)
	expect(pods[3], 2, 3, 6, 7)

	// Results come back in cluster order even though pods own non-contiguous clusters.
	_ = wsjson.Write(ctx, pods[2].ws, msg{T: "eval", ID: 1, Script: "c => c"})
	if r := <-pods[2].results; string(r.Data) != "[0,1,2,3,4,5,6,7]" {
		t.Fatalf("eval result %s", r.Data)
	}

	// Pod 0 returns: it gets its slot back right away and adopts slot 1, the lowest live slot's turn.
	pods[0] = connect("bot-0")
	expect(pods[0], 0, 1, 2, 3)
	expect(pods[2], 4, 5)
	expect(pods[3], 6, 7)
}
