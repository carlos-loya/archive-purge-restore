MODULE := github.com/carlos-loya/archive-purge-restore
BINARY := apr
MANAGER_BINARY := apr-manager
VERSION ?= dev
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

# Tooling
LOCALBIN := $(CURDIR)/bin
CONTROLLER_GEN := $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION := v0.18.0
ENVTEST := $(LOCALBIN)/setup-envtest
ENVTEST_VERSION := release-0.24
ENVTEST_K8S_VERSION := 1.32

.PHONY: all build build-manager test test-envtest clean lint \
	dev-up dev-down dev-reset test-integration test-all \
	manifests generate run-manager controller-gen envtest-tool \
	docker-build helm-sync-crds helm-lint \
	kind-tool kind-up kind-down kind-data-plane kind-load kind-install \
	test-k8s test-k8s-clean

all: build build-manager

##@ Build

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/apr

build-manager:
	go build $(LDFLAGS) -o $(MANAGER_BINARY) ./cmd/manager

##@ Test & lint

test:
	go test ./... -v

lint:
	go vet ./...

clean:
	rm -f $(BINARY) $(MANAGER_BINARY)
	rm -rf $(LOCALBIN)

##@ Operator code generation

# manifests regenerates CRDs and RBAC from kubebuilder markers.
manifests: controller-gen
	$(CONTROLLER_GEN) \
		rbac:roleName=manager-role \
		crd \
		paths="./api/..." \
		paths="./internal/controller/..." \
		output:crd:artifacts:config=config/crd/bases \
		output:rbac:artifacts:config=config/rbac

# generate regenerates DeepCopy methods.
generate: controller-gen
	$(CONTROLLER_GEN) \
		object:headerFile="hack/boilerplate.go.txt" \
		paths="./api/..."

controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

envtest-tool: $(ENVTEST)
$(ENVTEST):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

# test-envtest runs the controller tests against an envtest control plane.
# setup-envtest will download the kube-apiserver/etcd binaries on first use.
test-envtest: envtest-tool manifests generate
	@KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		go test ./internal/controller/... -v

##@ Container image + Helm chart

IMG ?= ghcr.io/carlos-loya/archive-purge-restore:dev

# docker-build builds the multi-stage Dockerfile. The same image contains
# both /apr (CLI + Job runtime) and /apr-manager (operator).
docker-build:
	docker build --build-arg VERSION=$(VERSION) -t $(IMG) .

# helm-sync-crds copies the controller-gen-generated CRDs into the Helm
# chart's crds/ directory. Run this any time the CRD types change.
helm-sync-crds: manifests
	cp config/crd/bases/*.yaml charts/apr/crds/

helm-lint: helm-sync-crds
	helm lint charts/apr

##@ kind end-to-end testing

KIND := $(LOCALBIN)/kind
KIND_VERSION := v0.27.0
KIND_CLUSTER := apr-test
KIND_NAMESPACE := apr-system

kind-tool: $(KIND)
$(KIND):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/kind@$(KIND_VERSION)

# kind-up creates a single-node cluster with NodePorts mapped to the host
# (postgres → 15432, minio → 19000) so the integration test can reach them.
kind-up: kind-tool
	$(KIND) get clusters | grep -qx $(KIND_CLUSTER) || \
		$(KIND) create cluster --name $(KIND_CLUSTER) --config dev/kind/kind-config.yaml --wait 60s

# kind-down deletes the cluster.
kind-down: kind-tool
	$(KIND) delete cluster --name $(KIND_CLUSTER)

# kind-data-plane deploys Postgres (with seed data) and MinIO (with a
# pre-created bucket) into the cluster.
kind-data-plane:
	kubectl apply -f dev/kind/namespace.yaml
	kubectl apply -f dev/kind/postgres.yaml
	kubectl apply -f dev/kind/minio.yaml
	kubectl -n data wait --for=condition=Available deploy/postgres deploy/minio --timeout=120s
	kubectl -n data wait --for=condition=Complete job/minio-bucket-init --timeout=120s

# kind-load builds the operator image and side-loads it into the kind node.
kind-load: docker-build kind-tool
	$(KIND) load docker-image $(IMG) --name $(KIND_CLUSTER)

# kind-install installs the chart against the loaded image. We explicitly
# apply CRDs first because Helm does not update them on upgrade (this
# matches the documented Helm CRD lifecycle).
kind-install: helm-sync-crds
	kubectl apply -f charts/apr/crds/
	helm upgrade --install apr ./charts/apr \
		--namespace $(KIND_NAMESPACE) \
		--create-namespace \
		--set image.repository=$$(echo $(IMG) | cut -d: -f1) \
		--set image.tag=$$(echo $(IMG) | cut -d: -f2)
	kubectl -n $(KIND_NAMESPACE) rollout restart deploy/apr-manager -n $(KIND_NAMESPACE) 2>/dev/null || true
	kubectl -n $(KIND_NAMESPACE) wait --for=condition=Available deploy --all --timeout=120s

# test-k8s assumes the cluster + data plane + operator are already up. Run
# `make kind-up kind-data-plane kind-load kind-install` first, or use the
# all-in-one target test-k8s-clean below.
test-k8s:
	go test -tags k8s ./integration/... -v -timeout 5m

# test-k8s-clean runs the full loop from a fresh cluster and tears it down.
# Useful for CI; slower than iterating against an existing cluster.
test-k8s-clean: kind-down kind-up kind-data-plane kind-load kind-install test-k8s kind-down

##@ Operator local run

# run-manager runs the manager out-of-cluster against your current kubeconfig.
run-manager: manifests generate build-manager
	./$(MANAGER_BINARY) --leader-elect=false

##@ Local databases (Docker)

dev-up:
	docker compose -f dev/docker-compose.yml up -d --wait

dev-down:
	docker compose -f dev/docker-compose.yml down -v

dev-reset: dev-down dev-up

test-integration: dev-up
	go test -tags integration ./integration/... -v

test-all: test test-integration
