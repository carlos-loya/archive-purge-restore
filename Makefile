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
	docker-build helm-sync-crds helm-lint

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
