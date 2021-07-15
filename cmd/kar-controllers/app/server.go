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

package app

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net/http"

	"github.com/IBM/multi-cluster-app-dispatcher/cmd/kar-controllers/app/options"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejob"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/health"

	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

func buildConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}

func Run(opt *options.ServerOption) error {
	config, err := buildConfig(opt.Master, opt.Kubeconfig)
	if err != nil {
		return err
	}

	neverStop := make(chan struct{})

	config.QPS = 100.0
	config.Burst = 200.0

	jobctrl := queuejob.NewJobController(config, opt)
	if jobctrl == nil {
		return nil
	}
	jobctrl.Run(neverStop)

	// This call is blocking (unless an error occurs) which equates to <-neverStop
	err = listenHealthProbe(opt)
	if err != nil {
		return err
	}

	return nil
}

// Starts the health probe listener
func listenHealthProbe(opt *options.ServerOption) error {
	handler := http.NewServeMux()
	handler.Handle("/healthz", &health.Handler{})
	err := http.ListenAndServe(opt.HealthProbeListenAddr, handler)
	if err != nil {
		return err
	}

	return nil
}
