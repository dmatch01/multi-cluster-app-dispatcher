/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SchedulingSpecPlural is the plural of SchedulingSpec
const SchedulingSpecPlural = "schedulingspecs"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SchedulingSpec struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec SchedulingSpecTemplate `json:"spec,omitempty" protobuf:"bytes,1,rep,name=spec"`
}

type ClusterReference struct {
	Name string `json:"name"`
}

type ClusterSchedulingSpec struct {
	Clusters        []ClusterReference    `json:"clusters,omitempty"`
	ClusterSelector *metav1.LabelSelector `json:"clusterSelector,omitempty"`
}

type ScheduleTimeSpec struct {
	Min     metav1.Time  `json:"minTimestamp,omitempty"`
	Desired metav1.Time  `json:"desiredTimestamp,omitempty"`
	Max     metav1.Time  `json:"maxTimestamp,omitempty"`
}

type RuntimeDurationSpec struct {
	Expected metav1.Duration `json:"expected,omitempty"`
	Limit    metav1.Duration `json:"limit,omitempty"`
}

type DispatchingWindowSpec struct {
	Start    ScheduleTimeSpec `json:"start,omitempty"`
	End      ScheduleTimeSpec `json:"end,omitempty"`
}

type SchedulingSpecTemplate struct {
	NodeSelector      map[string]string     `json:"nodeSelector,omitempty" protobuf:"bytes,1,rep,name=nodeSelector"`
	MinAvailable      int                   `json:"minAvailable,omitempty" protobuf:"bytes,2,rep,name=minAvailable"`
	ClusterScheduling ClusterSchedulingSpec `json:"clusterScheduling,omitempty"`
	DispatchingWindow DispatchingWindowSpec `json:"dispatchingWindow,omitempty"`
	RuntimeDuration   RuntimeDurationSpec   `json:"runtimeDuration,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SchedulingSpecList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []SchedulingSpec `json:"items"`
}

type ResourceName string
