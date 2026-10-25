FROM registry.access.redhat.com/ubi9/go-toolset:1.26.10-1792906297 AS builder
WORKDIR /opt/app-root/src
# Build context must be the repo root (gecko/).
# The replace directives in controllers/go.mod reference ../platform-api and ../orlop.
COPY orlop/go.mod orlop/go.sum orlop/
COPY platform-api/go.mod platform-api/go.sum platform-api/
COPY controllers/go.mod controllers/go.sum controllers/
RUN cd controllers && go mod download
COPY orlop/ orlop/
COPY platform-api/ platform-api/
COPY controllers/ controllers/
RUN cd controllers && CGO_ENABLED=0 GOOS=linux go build -o /opt/app-root/src/gecko-controllers .

FROM registry.access.redhat.com/ubi9/ubi-micro:9.8-1791895366

LABEL name="gecko-controllers" \
      com.redhat.component="gecko-controllers" \
      version="0.0.1" \
      release="1" \
      description="Gecko controllers for GCP HCP, managing reconciliation of ManagedHostedCluster and related resources." \
      io.k8s.description="Gecko controllers for GCP HCP." \
      summary="Gecko Controllers" \
      distribution-scope="private" \
      url="https://github.com/openshift-online/gecko" \
      vendor="Red Hat, Inc."

RUN mkdir /app && chown 65532:65532 /app
COPY --from=builder /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /opt/app-root/src/gecko-controllers /app/gecko-controllers
USER 65532:65532
WORKDIR /app
ENTRYPOINT ["/app/gecko-controllers"]
