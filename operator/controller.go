package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const label = "djshard.dev/cluster"

type reconciler struct {
	client.Client
	bridgeURL string // e.g. ws://djshard-operator.djshard-system.svc:8080
}

func authSecretName(sc string) string { return sc + "-djshard" }

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sc ShardCluster
	if err := r.Get(ctx, req.NamespacedName, &sc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: authSecretName(sc.Name), Namespace: sc.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if len(secret.Data["auth"]) == 0 {
			secret.Data = map[string][]byte{"auth": []byte(rand.Text())}
		}
		return controllerutil.SetControllerReference(&sc, secret, r.Scheme())
	}); err != nil {
		return ctrl.Result{}, err
	}

	total := sc.Spec.TotalShards
	if total == 0 {
		total = sc.Status.TotalShards
	}
	if total == 0 {
		// ponytail: "auto" is resolved once; set spec.totalShards to reshard later.
		n, err := r.recommendedShards(ctx, &sc)
		if err != nil {
			return ctrl.Result{}, err
		}
		total = n
	}
	if sc.Status.TotalShards != total {
		sc.Status.TotalShards = total
		if err := r.Status().Update(ctx, &sc); err != nil {
			return ctrl.Result{}, err
		}
	}

	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: sc.Name, Namespace: sc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		labels := map[string]string{label: sc.Name}
		tpl := sc.Spec.Template.DeepCopy()
		if len(tpl.Spec.Containers) == 0 {
			return errors.New("spec.template needs at least one container")
		}
		if tpl.Labels == nil {
			tpl.Labels = map[string]string{}
		}
		tpl.Labels[label] = sc.Name
		if tpl.Annotations == nil {
			tpl.Annotations = map[string]string{}
		}
		// Any change to the shard layout reassigns every pod, so roll them all.
		// ponytail: during the roll old and new pods may briefly own the same shard; Discord drops the older session.
		tpl.Annotations["djshard.dev/layout"] = fmt.Sprintf("%d/%d/%d", total, sc.Spec.ShardsPerCluster, sc.Spec.Replicas)

		c := &tpl.Spec.Containers[0]
		c.Env = append(c.Env,
			corev1.EnvVar{Name: "DJSHARD_BRIDGE_URL", Value: fmt.Sprintf("%s/%s/%s", r.bridgeURL, sc.Namespace, sc.Name)},
			corev1.EnvVar{Name: "DJSHARD_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			corev1.EnvVar{Name: "DJSHARD_AUTH", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: "auth"}}},
		)
		if sc.Spec.TokenSecretRef != nil {
			c.Env = append(c.Env, corev1.EnvVar{Name: "DISCORD_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: sc.Spec.TokenSecretRef}})
		}

		sts.Spec.Replicas = &sc.Spec.Replicas
		sts.Spec.ServiceName = sc.Name
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.Template = *tpl
		return controllerutil.SetControllerReference(&sc, sts, r.Scheme())
	})
	return ctrl.Result{}, err
}

func (r *reconciler) recommendedShards(ctx context.Context, sc *ShardCluster) (int, error) {
	ref := sc.Spec.TokenSecretRef
	if ref == nil {
		return 0, errors.New("totalShards is 0 and no tokenSecretRef to ask Discord with")
	}
	var s corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: sc.Namespace, Name: ref.Name}, &s); err != nil {
		return 0, err
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://discord.com/api/v10/gateway/bot", nil)
	req.Header.Set("Authorization", "Bot "+string(s.Data[ref.Key]))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET /gateway/bot: %s", res.Status)
	}
	var body struct{ Shards int }
	return body.Shards, json.NewDecoder(res.Body).Decode(&body)
}
