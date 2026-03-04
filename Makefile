# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN := $(shell go env GOPATH)/bin
else
GOBIN := $(shell go env GOBIN)
endif

GOPRIVATE = github.com/mydecisive/mdaiprovider
export GOPRIVATE

# Default versions (can be overridden via CLI)
OBSERVER_VERSION ?= 0.1.5
MDAICOL_VERSION ?= 0.1.6
MDAI_DD_COL_VERSION ?= 0.1.0
MDAI_DATA_FILTRATION_VERSION ?= 0.1.0

# Default image registry
REGISTRY ?= public.ecr.aws/decisiveai

# Supported components and their config/dockerfile
COLLECTORS := observer-collector mdai-collector mdai-datadog-collector mdai-data-filtration-collector

# Map components to config paths and Dockerfiles
CONFIG_observer-collector = config/observer-collector/observer-collector-builder.yaml
CONFIG_mdai-collector = config/mdai-collector/mdai-collector-builder.yaml
CONFIG_mdai-datadog-collector = config/mdai-datadog-collector/mdai-datadog-collector-builder.yaml
CONFIG_mdai-data-filtration-collector = config/mdai-data-filtration-collector/mdai-data-filtration-collector-builder.yaml

DOCKERFILE_observer-collector = docker/Dockerfile
DOCKERFILE_mdai-collector = docker/mdai-collector.Dockerfile
DOCKERFILE_mdai-datadog-collector = docker/mdai-datadog-collector.Dockerfile
DOCKERFILE_mdai-data-filtration-collector = docker/mdai-data-filtration-collector.Dockerfile

VERSION_observer-collector = $(OBSERVER_VERSION)
VERSION_mdai-collector = $(MDAICOL_VERSION)
VERSION_mdai-datadog-collector = $(MDAI_DD_COL_VERSION)
VERSION_mdai-data-filtration-collector = $(MDAI_DATA_FILTRATION_VERSION)

# Resolve values dynamically based on component
CONFIG := $(CONFIG_$(COLLECTOR))
DOCKERFILE := $(DOCKERFILE_$(COLLECTOR))
VERSION := $(VERSION_$(COLLECTOR))
IMAGE := $(REGISTRY)/$(COLLECTOR):$(VERSION)

require-component = \
	@if [ -z "$(COLLECTOR)" ]; then \
		echo "🧩 You must specify a component, e.g.:"; \
		echo "    make $@ COLLECTOR=mdai-collector"; \
		echo; \
		echo "Available components:"; \
		for c in $(COLLECTORS); do echo " - $$c"; done; \
		exit 1; \
	fi

.PHONY: all
all: build

.PHONY: build
build:
	git config --global url."ssh://git@github.com".insteadOf https://github.com
	$(require-component)
	@echo "🔨 Building $(COLLECTOR) from $(CONFIG)"
	builder --config=$(CONFIG)

.PHONY: docker-build
docker-build:
	# TODO: inject ssh key variable
	$(require-component)
	@echo "🐳 Building Docker image for $(COLLECTOR)"
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t $(IMAGE) \
		-f $(DOCKERFILE) \
		--load .

.PHONY: docker-push
docker-push:
	$(require-component)
	@echo "🚀 Pushing Docker image for $(COLLECTOR)"
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t $(IMAGE) \
		-f $(DOCKERFILE) \
		--push .

.PHONY: list
list:
	@echo "Available components:"
	@$(foreach c,$(COLLECTORS),echo " - $(c)";)
