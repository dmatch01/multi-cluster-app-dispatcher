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

package provider

import (
	v1 "k8s.io/api/core/v1"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/golang/glog"

	apierr "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/metrics/pkg/apis/custom_metrics"
	"k8s.io/metrics/pkg/apis/external_metrics"

	clusterstatecache "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/clusterstate/cache"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/metrics/provider"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/metrics/provider/helpers"
)

// CustomMetricResource wraps provider.CustomMetricInfo in a struct which stores the Name and Namespace of the resource
// So that we can accurately store and retrieve the metric as if this were an actual metrics server.
type CustomMetricResource struct {
	provider.CustomMetricInfo
	types.NamespacedName
}

// externalMetric provides examples for metrics which would otherwise be reported from an external source
// TODO (damemi): add dynamic external metrics instead of just hardcoded examples
type ExternalMetric struct {
	info   provider.ExternalMetricInfo
	labels map[string]string
	Value  external_metrics.ExternalMetricValue
}

var (
	testingExternalMetrics = []ExternalMetric{
		{
			info: provider.ExternalMetricInfo{
				Metric: "my-external-metric",
			},
			labels: map[string]string{"foo": "bar"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "my-external-metric",
				MetricLabels: map[string]string{
					"foo": "bar",
				},
				Value: *resource.NewQuantity(42, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "my-external-metric",
			},
			labels: map[string]string{"foo": "baz"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "my-external-metric",
				MetricLabels: map[string]string{
					"foo": "baz",
				},
				Value: *resource.NewQuantity(43, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "cluster-external-metric",
			},
			labels: map[string]string{"cluster": "cpu"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "cluster-external-metric",
				MetricLabels: map[string]string{
					"cluster": "cpu",
				},
				Value: *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "cluster-external-metric",
			},
			labels: map[string]string{"cluster": "memory"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "cluster-external-metric",
				MetricLabels: map[string]string{
					"cluster": "memory",
				},
				Value: *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "queue-external-metric",
			},
			labels: map[string]string{"cluster": "queue-count"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "queue-external-metric",
				MetricLabels: map[string]string{
					"cluster": "queue-count",
				},
				Value: *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "cluster-capacity-external-metric",
			},
			labels: map[string]string{"cluster": "cpu-capacity"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "cluster-capacity-external-metric",
				MetricLabels: map[string]string{
					"cluster": "cpu-capacity",
				},
				Value: *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "cluster-capacity-external-metric",
			},
			labels: map[string]string{"cluster": "memory-capacity"},
			Value: external_metrics.ExternalMetricValue{
				MetricName: "cluster-capacity-external-metric",
				MetricLabels: map[string]string{
					"cluster": "memory-capacity",
				},
				Value: *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		{
			info: provider.ExternalMetricInfo{
				Metric: "other-external-metric",
			},
			labels: map[string]string{},
			Value: external_metrics.ExternalMetricValue{
				MetricName:   "other-external-metric",
				MetricLabels: map[string]string{},
				Value:        *resource.NewQuantity(44, resource.DecimalSI),
			},
		},
	}
)

// testingProvider is a sample implementation of provider.MetricsProvider which stores a map of fake metrics
type testingProvider struct {
	client dynamic.Interface
	mapper apimeta.RESTMapper

	valuesLock      sync.RWMutex
	values          map[CustomMetricResource]resource.Quantity
	externalMetrics []ExternalMetric
	cache2		clusterstatecache.Cache

}

// NewFakeProvider returns an instance of testingProvider, along with its restful.WebService that opens endpoints to post new fake metrics
func NewFakeProvider(client dynamic.Interface, mapper apimeta.RESTMapper, clusterStateCache clusterstatecache.Cache) (provider.MetricsProvider, *restful.WebService) {
	glog.V(10).Infof("Entered NewFakeProvider()")
	provider := &testingProvider{
		client:          client,
		mapper:          mapper,
		values:          make(map[CustomMetricResource]resource.Quantity),
		externalMetrics: testingExternalMetrics,
		cache2:		clusterStateCache,
	}
	return provider, provider.webService()
}

// webService creates a restful.WebService with routes set up for receiving fake metrics
// These writing routes have been set up to be identical to the format of routes which metrics are read from.
// There are 3 metric types available: namespaced, root-scoped, and namespaces.
// (Note: Namespaces, we're assuming, are themselves namespaced resources, but for consistency with how metrics are retreived they have a separate route)
func (p *testingProvider) webService() *restful.WebService {
	glog.V(10).Infof("Entered webService()")
	ws := new(restful.WebService)

	ws.Path("/write-metrics")

	// Namespaced resources
	ws.Route(ws.POST("/namespaces/{namespace}/{resourceType}/{name}/{metric}").To(p.updateMetric).
		Param(ws.BodyParameter("Value", "Value to set metric").DataType("integer").DefaultValue("0")))

	// Root-scoped resources
	ws.Route(ws.POST("/{resourceType}/{name}/{metric}").To(p.updateMetric).
		Param(ws.BodyParameter("Value", "Value to set metric").DataType("integer").DefaultValue("0")))

	// Namespaces, where {resourceType} == "namespaces" to match API
	ws.Route(ws.POST("/{resourceType}/{name}/metrics/{metric}").To(p.updateMetric).
		Param(ws.BodyParameter("Value", "Value to set metric").DataType("integer").DefaultValue("0")))
	return ws
}

// updateMetric writes the metric provided by a restful request and stores it in memory
func (p *testingProvider) updateMetric(request *restful.Request, response *restful.Response) {
	glog.V(10).Infof("Entered updateMetric()")
	p.valuesLock.Lock()
	defer p.valuesLock.Unlock()

	namespace := request.PathParameter("namespace")
	glog.V(9).Infof("Namespace=%s", namespace)
	resourceType := request.PathParameter("resourceType")
	glog.V(9).Infof("Resource type=%s", resourceType)
	namespaced := false
	if len(namespace) > 0 || resourceType == "namespaces" {
		namespaced = true
		glog.V(9).Infof("Namespaced=true")
	}
	name := request.PathParameter("name")
	glog.V(9).Infof("Name=%s", name)
	metricName := request.PathParameter("metric")
	glog.V(9).Infof("MetricName=%s", metricName)

	value := new(resource.Quantity)
	err := request.ReadEntity(value)
	glog.V(9).Infof("Value=%v", value)
	if err != nil {
		response.WriteErrorString(http.StatusBadRequest, err.Error())
		glog.V(10).Infof("Bad Value: %v", value)
		return
	}

	groupResource := schema.ParseGroupResource(resourceType)

	info := provider.CustomMetricInfo{
		GroupResource: groupResource,
		Metric:        metricName,
		Namespaced:    namespaced,
	}

	info, _, err = info.Normalized(p.mapper)
	if err != nil {
		glog.Errorf("Error normalizing info: %s", err)
	}
	namespacedName := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	metricInfo := CustomMetricResource{
		CustomMetricInfo: info,
		NamespacedName:   namespacedName,
	}
	p.values[metricInfo] = *value
}

// valueFor is a helper function to get just the Value of a specific metric
func (p *testingProvider) valueFor(info provider.CustomMetricInfo, name types.NamespacedName) (resource.Quantity, error) {
	glog.V(10).Infof("Entered valueFor()")
	info, _, err := info.Normalized(p.mapper)
	if err != nil {
		return resource.Quantity{}, err
	}
	metricInfo := CustomMetricResource{
		CustomMetricInfo: info,
		NamespacedName:   name,
	}

	value, found := p.values[metricInfo]
	if !found {
		return resource.Quantity{}, provider.NewMetricNotFoundForError(info.GroupResource, info.Metric, name.Name)
	}

	glog.Infof("valueFor(): metricInfo=%v, Value=%v", metricInfo, value)
	return value, nil
}

// metricFor is a helper function which formats a Value, metric, and object info into a MetricValue which can be returned by the metrics API
func (p *testingProvider) metricFor(value resource.Quantity, name types.NamespacedName, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	glog.V(10).Infof("Entered metricFor()")
	objRef, err := helpers.ReferenceFor(p.mapper, name, info)
	if err != nil {
		return nil, err
	}

	metric := &custom_metrics.MetricValue{
		DescribedObject: objRef,
		Metric: custom_metrics.MetricIdentifier{
			Name: info.Metric,
		},
		Timestamp: metav1.Time{time.Now()},
		Value:     value,
	}

	if len(selector.String()) > 0 {
		labelSelector, err := metav1.ParseToLabelSelector(selector.String())
		if err != nil {
			return nil, err
		}
		metric.Metric.Selector = labelSelector
	}
	glog.V(9).Infof("Metric Value=%v", metric)
	return metric, nil
}

// metricsFor is a wrapper used by GetMetricBySelector to format several metrics which match a resource selector
func (p *testingProvider) metricsFor(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
	glog.V(10).Infof("Entered metricFor()")
	names, err := helpers.ListObjectNames(p.mapper, p.client, namespace, selector, info)
	if err != nil {
		return nil, err
	}

	res := make([]custom_metrics.MetricValue, 0, len(names))
	for _, name := range names {
		namespacedName := types.NamespacedName{Name: name, Namespace: namespace}
		value, err := p.valueFor(info, namespacedName)
		if err != nil {
			if apierr.IsNotFound(err) {
				continue
			}
			return nil, err
		}

		metric, err := p.metricFor(value, namespacedName, selector, info)
		if err != nil {
			return nil, err
		}
		res = append(res, *metric)
	}
	glog.V(9).Infof("Metric Value: res=%v", res)

	return &custom_metrics.MetricValueList{
		Items: res,
	}, nil
}

func (p *testingProvider) GetMetricByName(name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	glog.V(10).Infof("Entered GetMetricByName()")
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	value, err := p.valueFor(info, name)
	glog.Infof("GetMetricByName(): info=%v, name=%v, value=%v", info, name, value)

	if err != nil {
		return nil, err
	}
	return p.metricFor(value, name, labels.Everything(), info)
}

func (p *testingProvider) GetMetricBySelector(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
	glog.V(10).Infof("Entered GetMetricBySelector()")
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	return p.metricsFor(namespace, selector, info)
}

func (p *testingProvider) ListAllMetrics() []provider.CustomMetricInfo {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	// Get unique CustomMetricInfos from wrapper CustomMetricResources
	infos := make(map[provider.CustomMetricInfo]struct{})
	for resource := range p.values {
		infos[resource.CustomMetricInfo] = struct{}{}
	}

	// Build slice of CustomMetricInfos to be returns
	metrics := make([]provider.CustomMetricInfo, 0, len(infos))
	for info := range infos {
		metrics = append(metrics, info)
	}

	return metrics
}


// Hack to dynamically load metrics
func (p *testingProvider) generateQueueSpecs(clusterName string, jobname string,
		runtime metav1.Duration, desiredStartTime metav1.Time, resources v1.ResourceRequirements) (external_metrics.ExternalMetricValue,
		external_metrics.ExternalMetricValue){
	cpuMetric := external_metrics.ExternalMetricValue{}
	memMetric := external_metrics.ExternalMetricValue{}
	cpuMetric.MetricName = "queue-external-metric"
	memMetric.MetricName = "queue-external-metric"
	window := runtime.Duration.String()
	startTime := desiredStartTime.Format(time.RFC3339)
	cpuLabels := map[string]string {
		"clusterID": clusterName,
		"jobID": jobname,
		"resource": "cpu",
		"starttimestamp": startTime,
		"window": window,
	}
	memLabels := map[string]string {
		"clusterID": clusterName,
		"jobID": jobname,
		"resource": "memory",
		"starttimestamp": startTime,
		"window": window,
	}
	cpuMetric.MetricLabels = cpuLabels
	memMetric.MetricLabels = memLabels
	cpuMetric.Timestamp = metav1.Now()
	cpuMetric.Value = resources.Requests.Cpu().DeepCopy()
	memMetric.Value = resources.Requests.Memory().DeepCopy()
	return cpuMetric, memMetric
}
// Hack to dynamically load metrics
func (p *testingProvider) generateQueueSpecsAdapter(clusternum int, jobname string, runtime metav1.Duration) (external_metrics.ExternalMetricValue, external_metrics.ExternalMetricValue) {
	clusterName := "cluster" + strconv.Itoa(clusternum)
	desiredStartTime := metav1.Now()

	requests := map[v1.ResourceName]resource.Quantity{}
	requests[v1.ResourceMemory] = *resource.NewQuantity(int64(1000000000), resource.DecimalSI)
	requests[v1.ResourceCPU] = *resource.NewQuantity(int64(1000), resource.DecimalSI)
	aggregatedResources := v1.ResourceRequirements{
		Requests: requests,
	}
	return p.generateQueueSpecs(clusterName, jobname, runtime, desiredStartTime, aggregatedResources)
}

// Hack to dynamically load cluster capacity metrics
func (p *testingProvider) generateClusterCapacitySpecs(clusternum int, resourceType string, resourceAmount int) external_metrics.ExternalMetricValue{
	metric := external_metrics.ExternalMetricValue{}
	metric.MetricName = "cluster-capacity-external-metric"
	clusterName := "cluster" + strconv.Itoa(clusternum)
	metricLabels := map[string]string {
		"clusterID": clusterName,
		"resource": resourceType,
	}
	metric.MetricLabels = metricLabels
	metric.Timestamp = metav1.Now()
	metric.Value = *resource.NewQuantity(int64(resourceAmount), resource.DecimalSI)
	return metric
}

// Hack to dynamically load queue metrics
func (p *testingProvider) loadDynamicQueueMetrics(matchingMetrics []external_metrics.ExternalMetricValue,
				) []external_metrics.ExternalMetricValue {
	runtime := metav1.Duration{ 600 * time.Second}
	cpuMetric, memoryMetric := p.generateQueueSpecsAdapter(1,"default/job1", runtime)
	matchingMetrics = append(matchingMetrics, cpuMetric)
	matchingMetrics = append(matchingMetrics, memoryMetric)

	runtime = metav1.Duration{ 36000 * time.Second}
	cpuMetric, memoryMetric = p.generateQueueSpecsAdapter(1,"default/job2",runtime)
	matchingMetrics = append(matchingMetrics, cpuMetric)
	matchingMetrics = append(matchingMetrics, memoryMetric)

	jobs := p.cache2.GetJobsQueued()
	for _, job := range jobs {
		jobname := job.Namespace + "/" + job.Name
		clusterName := "UnknownCluster"
		if len(job.Spec.SchedSpec.ClusterScheduling.Clusters) > 0 {
			clusterName = job.Spec.SchedSpec.ClusterScheduling.Clusters[0].Name
		}
		runtime := job.Spec.SchedSpec.RuntimeDuration.Expected
		aggregatedResources := job.Status.AggregatedResources
		desiredStartTime := job.Spec.SchedSpec.DispatchingWindow.Start.Desired
		cpuMetric, memoryMetric = p.generateQueueSpecs(clusterName, jobname, runtime, desiredStartTime, aggregatedResources)
		matchingMetrics = append(matchingMetrics, cpuMetric)
		matchingMetrics = append(matchingMetrics, memoryMetric)
	}
	return matchingMetrics
}

// Hack to dynamically load cluster memory capacity metrics
func (p *testingProvider) loadDynamicClusterCapacityMemoryMetrics(
	              matchingMetrics []external_metrics.ExternalMetricValue) []external_metrics.ExternalMetricValue {

	memoryMetric := p.generateClusterCapacitySpecs(1, "memory",10000000000)
	matchingMetrics = append(matchingMetrics, memoryMetric)


	memoryMetric = p.generateClusterCapacitySpecs(2, "memory",10000000000)
	matchingMetrics = append(matchingMetrics, memoryMetric)

	return matchingMetrics
}
// Hack to dynamically load cluster memory capacity metrics
func (p *testingProvider) loadDynamicClusterCapacityCpuMetrics(
	matchingMetrics []external_metrics.ExternalMetricValue) []external_metrics.ExternalMetricValue {

	cpuMetric := p.generateClusterCapacitySpecs(1, "cpu",100000)
	matchingMetrics = append(matchingMetrics, cpuMetric)

	cpuMetric = p.generateClusterCapacitySpecs(2, "cpu",100000)
	matchingMetrics = append(matchingMetrics, cpuMetric)
	return matchingMetrics
}

func (p *testingProvider) GetExternalMetric(namespace string, metricSelector labels.Selector,
				info provider.ExternalMetricInfo) (*external_metrics.ExternalMetricValueList, error) {
	glog.V(10).Infof("Entered GetExternalMetric()")
	glog.V(9).Infof("metricsSelector: %s, metricsInfo: %s", metricSelector.String(), info.Metric)
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	matchingMetrics := []external_metrics.ExternalMetricValue{}
	for _, metric := range p.externalMetrics {
		glog.V(9).Infof("externalMetricsInfo: %s, externalMetricValue: %v, externalMetricLabels: %v ",
									metric.info.Metric, metric.Value, metric.labels)
		if metric.info.Metric == info.Metric && metricSelector.Matches(labels.Set(metric.labels)) {
			metricValue := metric.Value
			labelVal := metric.labels["cluster"]
			glog.V(9).Infof("'cluster label value: %s, ", labelVal)
			// Set memory Value
			if strings.Compare(labelVal, "memory") == 0  {
				resources := p.cache2.GetUnallocatedResources()
				glog.V(9).Infof("Cache resources: %v", resources)

				glog.V(10).Infof("Setting memory metric Value: %f.", resources.Memory)
				metricValue.Value = *resource.NewQuantity(int64(resources.Memory), resource.DecimalSI)
				//metricValue.Value = *resource.NewQuantity(4500000000, resource.DecimalSI)
			} else if strings.Compare(labelVal, "cpu") == 0 {
				// Set cpu Value
				resources := p.cache2.GetUnallocatedResources()
				glog.V(9).Infof("Cache resources: %f", resources)

				glog.V(10).Infof("Setting cpu metric Value: %v.", resources.MilliCPU)
				metricValue.Value = *resource.NewQuantity(int64(resources.MilliCPU), resource.DecimalSI)
			} else if strings.Compare(labelVal, "queue-count") == 0 {
				// Set size of queue Value
				//resources := p.cache2.GetUnallocatedResources()

				glog.V(10).Infof("Setting queue size metric Value: %f.", 9)
				metricValue.Value = *resource.NewQuantity(int64(9), resource.DecimalSI)

				//Hack to set dynamic metrics
				matchingMetrics = p.loadDynamicQueueMetrics(matchingMetrics)
				continue
			} else if strings.Compare(labelVal, "memory-capacity") == 0 {
				// Set cpu Value
				//resources := p.cache2.GetUnallocatedResources()
				//glog.V(9).Infof("Cache resources: %f", resources)

				glog.V(10).Infof("Setting memory-capacity metric Value: %v.", 7)
				metricValue.Value = *resource.NewQuantity(int64(7), resource.DecimalSI)

				matchingMetrics = p.loadDynamicClusterCapacityMemoryMetrics(matchingMetrics)
				continue
			} else if strings.Compare(labelVal, "cpu-capacity") == 0 {
				// Set cpu Value
				//resources := p.cache2.GetUnallocatedResources()
				//glog.V(9).Infof("Cache resources: %f", resources)

				glog.V(10).Infof("Setting cpu-capacity metric Value: %v.", 8)
				metricValue.Value = *resource.NewQuantity(int64(8), resource.DecimalSI)

				matchingMetrics = p.loadDynamicClusterCapacityCpuMetrics(matchingMetrics)
				continue

			} else {
				glog.V(10).Infof("Not setting cpu/memory/queue-size metric Value")
			}

			metricValue.Timestamp = metav1.Now()
			matchingMetrics = append(matchingMetrics, metricValue)
		}
	}
	return &external_metrics.ExternalMetricValueList{
		Items: matchingMetrics,
	}, nil
}

func (p *testingProvider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
	glog.V(10).Infof("Entered ListAllExternalMetrics()")
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	externalMetricsInfo := []provider.ExternalMetricInfo{}
	for _, metric := range p.externalMetrics {
		externalMetricsInfo = append(externalMetricsInfo, metric.info)
		glog.V(9).Infof("Add metric=%v to externalMetricsInfo", metric)
	}
	glog.V(9).Infof("ExternalMetricsInfo=%v", externalMetricsInfo)
	return externalMetricsInfo
}
