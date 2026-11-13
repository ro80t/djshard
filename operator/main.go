package main

import (
	"context"
	"flag"
	"net/http"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func main() {
	listen := flag.String("bridge-listen", ":8080", "bridge WebSocket listen address")
	url := flag.String("bridge-url", os.Getenv("BRIDGE_URL"), "bridge URL as seen from bot pods")
	flag.Parse()
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("djshard")

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(addToScheme(scheme))

	// No leader election: the bridge keeps live connections in memory, so run exactly one replica.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// Don't cache (and watch) every Secret in the cluster; we only read a few.
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}},
	})
	must(err)

	must(ctrl.NewControllerManagedBy(mgr).
		For(&ShardCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(&reconciler{Client: mgr.GetClient(), bridgeURL: *url}))

	mux := http.NewServeMux()
	mux.Handle("GET /{ns}/{name}", &bridge{cache: mgr.GetClient(), direct: mgr.GetAPIReader(), groups: map[string]map[int]*conn{}})
	srv := &http.Server{Addr: *listen, Handler: mux}
	must(mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		go func() { <-ctx.Done(); srv.Close() }()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})))

	log.Info("starting", "bridge", *listen)
	must(mgr.Start(ctrl.SetupSignalHandler()))
}

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "fatal")
		os.Exit(1)
	}
}
