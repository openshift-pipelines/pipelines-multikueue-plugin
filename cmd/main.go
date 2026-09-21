package main

import (
	"github.com/openshift-pipelines/pipelines-multikueue-plugin/pkg/controllers"

	"knative.dev/pkg/injection/sharedmain"
)

func main() {
	sharedmain.Main("pipelines-multikueue-plugin", controllers.NewController())
}
