package main

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var groupVersion = schema.GroupVersion{Group: "djshard.ro80t.com", Version: "v1alpha1"}

func addToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(groupVersion, &ShardCluster{}, &ShardClusterList{})
	metav1.AddToGroupVersion(s, groupVersion)
	return nil
}

// Defaults and validation live in config/crd.yaml.
type ShardClusterSpec struct {
	// Pods; clusters are split evenly across them.
	Replicas int32 `json:"replicas"`
	// 0 = Discord's recommended count (needs TokenSecretRef), fetched once.
	TotalShards      int `json:"totalShards"`
	ShardsPerCluster int `json:"shardsPerCluster"`
	// Injected as DISCORD_TOKEN into the first container.
	TokenSecretRef *corev1.SecretKeySelector `json:"tokenSecretRef,omitempty"`
	// Pod template; the first container runs the ClusterManager.
	Template corev1.PodTemplateSpec `json:"template"`
}

type ShardClusterStatus struct {
	TotalShards int `json:"totalShards,omitempty"`
}

type ShardCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ShardClusterSpec   `json:"spec"`
	Status            ShardClusterStatus `json:"status,omitempty"`
}

type ShardClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardCluster `json:"items"`
}

// Hand-written instead of controller-gen: only these two types need it.
func (in *ShardCluster) DeepCopyInto(out *ShardCluster) {
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.Template.DeepCopyInto(&out.Spec.Template)
	if in.Spec.TokenSecretRef != nil {
		out.Spec.TokenSecretRef = in.Spec.TokenSecretRef.DeepCopy()
	}
}

func (in *ShardCluster) DeepCopyObject() runtime.Object {
	out := new(ShardCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *ShardClusterList) DeepCopyObject() runtime.Object {
	out := &ShardClusterList{TypeMeta: in.TypeMeta}
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]ShardCluster, len(in.Items))
	for i := range in.Items {
		in.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}
