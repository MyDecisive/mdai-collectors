FROM alpine:3.19 AS certs
RUN apk --update add ca-certificates

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder
WORKDIR /app
COPY . .
ARG OTEL_VERSION=0.139.0
ARG BUILDOS=linux
ARG BUILDARCH=amd64
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN curl --proto '=https' --tlsv1.2 -fL https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/cmd%2Fbuilder%2Fv${OTEL_VERSION}/ocb_${OTEL_VERSION}_${BUILDOS}_${BUILDARCH} -o /app/builder && chmod +x /app/builder
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} /app/builder --config=config/mdai-datadog-collector/mdai-datadog-collector-builder.yaml

FROM scratch
ARG VARIANT
COPY --from=builder app/mdai-datadog-collector /
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE 4317/tcp 4318/tcp 8126/tcp 8891/tcp 8899/tcp 13133/tcp
USER 65533:65533
ENTRYPOINT ["/mdai-datadog-collector"]
