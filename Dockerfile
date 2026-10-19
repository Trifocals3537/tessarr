# check=error=true

# xx provides cross-compilation toolchains for CGO builds
FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

# Stage 1: Build binaries — pinned to BUILDPLATFORM so Go runs natively (fast)
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM
ARG VERSION=0.0.0
ARG CHANNEL=dev

# Copy xx scripts for cross-compilation
COPY --from=xx / /

WORKDIR /app

# Install cross-compilation toolchain via xx
RUN apk add --no-cache clang lld && \
    xx-apk add --no-cache gcc g++ musl-dev libc-dev fuse-dev

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

COPY . .

# Build main binary — xx-go sets CC/CXX/GOOS/GOARCH automatically
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 \
    xx-go build -trimpath \
    -ldflags="-w -s -X github.com/Trifocals3537/tessarr/pkg/version.Version=${VERSION} -X github.com/Trifocals3537/tessarr/pkg/version.Channel=${CHANNEL}" \
    -o /tessarr && \
    xx-verify /tessarr

# Build healthcheck (no CGO needed, plain cross-compile)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-w -s" \
    -o /healthcheck cmd/healthcheck/main.go

# Stage 2: Final image
FROM alpine:latest

ARG VERSION=0.0.0
ARG CHANNEL=dev

LABEL version="${VERSION}-${CHANNEL}"
LABEL org.opencontainers.image.source="https://github.com/Trifocals3537/tessarr"
LABEL org.opencontainers.image.title="Tessarr"
LABEL org.opencontainers.image.description="Self-hosted debrid and Usenet media bridge with qBittorrent and SABnzbd-compatible APIs"
LABEL org.opencontainers.image.authors="Tessarr contributors"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.documentation="https://github.com/Trifocals3537/tessarr/blob/beta/README.md"

# Install dependencies including rclone (from binary)
RUN apk add --no-cache fuse3 ca-certificates su-exec shadow curl unzip tzdata && \
    echo "user_allow_other" >> /etc/fuse.conf && \
    case "$(uname -m)" in \
        x86_64) ARCH=amd64 ;; \
        aarch64) ARCH=arm64 ;; \
        armv7l|armv7) ARCH=arm ;; \
        *) echo "Unsupported architecture: $(uname -m)" && exit 1 ;; \
    esac && \
    curl -O "https://downloads.rclone.org/rclone-current-linux-${ARCH}.zip" && \
    unzip "rclone-current-linux-${ARCH}.zip" && \
    cp rclone-*/rclone /usr/local/bin/ && \
    chmod +x /usr/local/bin/rclone && \
    rm -rf rclone-* && \
    apk del curl unzip

# Copy binaries and entrypoint
COPY --from=builder /tessarr /usr/bin/tessarr
COPY --from=builder /healthcheck /usr/bin/healthcheck
COPY scripts/container-defaults.sh /container-defaults.sh
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /container-defaults.sh /entrypoint.sh

# Set environment variables
ENV PUID=1000
ENV PGID=1000
ENV LOG_PATH=/app/logs
ENV TESSARR_BIND_ADDRESS=0.0.0.0

EXPOSE 8282
VOLUME ["/app"]

HEALTHCHECK --interval=10s --timeout=35s --start-period=120s --retries=10 CMD ["/container-defaults.sh", "/usr/bin/healthcheck", "--config", "/app"]

ENTRYPOINT ["/container-defaults.sh", "/entrypoint.sh"]
CMD ["/usr/bin/tessarr", "--config", "/app"]
