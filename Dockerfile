# Build the controller binary
FROM registry.access.redhat.com/ubi9/go-toolset:1.26@sha256:5e231f8c5eab7812a1e2c701ce48c63eb3bb02dc9f372d8fbb59e3e1cd9c493c AS builder
ARG TARGETOS
ARG TARGETARCH

ENV GOTOOLCHAIN=auto
WORKDIR /opt/app-root/src

# Coverage instrumentation build argument
ARG ENABLE_COVERAGE=false

# Copy the go source
COPY . .

# Build with or without coverage instrumentation
RUN if [ "$ENABLE_COVERAGE" = "true" ]; then \
        echo "Building with coverage instrumentation..."; \
        CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -buildvcs=false -cover -covermode=atomic -tags=coverage -o controller ./cmd/; \
    else \
        echo "Building production binary..."; \
        CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -buildvcs=false -a -o controller ./cmd/; \
    fi


FROM  registry.access.redhat.com/ubi9-micro:latest@sha256:b1e86b97028b8fcfb6d85f997c39e6b6b67496163ef8d80d243220a4918e8bef   
WORKDIR /
COPY --from=builder /opt/app-root/src/controller .
COPY LICENSE /licenses/
USER 65532:65532

# It is mandatory to set these labels
LABEL \
    com.redhat.component="openshift-controller-rhel9-container" \
    cpe="cpe:/a:redhat:openshift_pipelines:0.1::el9" \
    description="Red Hat OpenShift Pipelines pipelines-multikueue-plugin controller" \
    io.k8s.description="Red Hat OpenShift Pipelines pipelines-multikueue-plugin controller" \
    io.k8s.display-name="Red Hat OpenShift Pipelines pipelines-multikueue-plugin controller" \
    io.openshift.tags="tekton,openshift,pipelines-multikueue-plugin,controller" \
    maintainer="pipelines-extcomm@redhat.com" \
    name="openshift-pipelines/controller-rhel9" \
    summary="Red Hat OpenShift Pipelines pipelines-multikueue-plugin controller" \
    version="v0.1.1"

ENTRYPOINT ["/controller"]
