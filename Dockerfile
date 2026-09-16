FROM babashka/babashka:1.12.218 AS bb
FROM ubuntu:24.04 AS base
ARG TARGETARCH
ARG TOFU_VERSION=1.11.5
ARG KUBECTL_VERSION=v1.36.3
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip git openssh-client ansible python3 python3-boto3 \
    redis-tools bash coreutils procps util-linux openjdk-21-jre-headless \
 && rm -rf /var/lib/apt/lists/*
COPY --from=bb /usr/local/bin/bb /usr/local/bin/bb
# OpenTofu and kubectl are verified against their published checksums. AWS
# publishes a PGP signature (.sig) for the CLI archive but no checksum file
# addressable by URL, and the unversioned URL moves with every release, so
# the image digest is what pins that part of the toolchain.
RUN arch="${TARGETARCH:-amd64}" \
 && curl -fsSL "https://github.com/opentofu/opentofu/releases/download/v${TOFU_VERSION}/tofu_${TOFU_VERSION}_linux_${arch}.zip" -o /tmp/tofu.zip \
 && curl -fsSL "https://github.com/opentofu/opentofu/releases/download/v${TOFU_VERSION}/tofu_${TOFU_VERSION}_SHA256SUMS" -o /tmp/tofu-sums \
 && expected="$(awk -v file="tofu_${TOFU_VERSION}_linux_${arch}.zip" '$2 == file {print $1}' /tmp/tofu-sums)" \
 && test -n "$expected" && echo "$expected  /tmp/tofu.zip" | sha256sum -c - \
 && unzip /tmp/tofu.zip tofu -d /usr/local/bin && rm /tmp/tofu.zip /tmp/tofu-sums \
 && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}/kubectl" -o /usr/local/bin/kubectl \
 && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}/kubectl.sha256" -o /tmp/kubectl.sha256 \
 && echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c - \
 && chmod +x /usr/local/bin/kubectl \
 && case "$arch" in amd64) awsarch=x86_64 ;; arm64) awsarch=aarch64 ;; *) exit 1 ;; esac \
 && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${awsarch}.zip" -o /tmp/aws.zip \
 && unzip -q /tmp/aws.zip -d /tmp && /tmp/aws/install && rm -rf /tmp/aws /tmp/aws.zip
WORKDIR /app

FROM base AS green
COPY deps.edn bb.edn ./
COPY src ./src
COPY test ./test
# Run bb test on the build host before cross-building. Babashka native-image
# does not execute reliably under QEMU; the controller runs on native workers.
ENV COLORS_WORKDIR=/data/work
ENTRYPOINT ["bb", "controller", "--in-cluster"]
CMD ["colors-dev"]

FROM oven/bun:1.3.10 AS bun
FROM ghcr.io/astral-sh/uv:0.10.2 AS uv
FROM base AS red
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY package.json ./
COPY red/package.json red/bun.lock ./red/
RUN cd red && bun install --frozen-lockfile
COPY red/src ./red/src
COPY resources ./resources
ENV COLORS_WORKDIR=/data/work
ENTRYPOINT ["bun", "/app/red/src/controller.ts", "--in-cluster"]
CMD ["colors-dev"]

FROM base AS blue
COPY --from=uv /uv /usr/local/bin/uv
COPY blue/pyproject.toml blue/uv.lock ./blue/
COPY blue/src ./blue/src
RUN cd blue && uv sync --frozen --no-dev
ENV COLORS_WORKDIR=/data/work PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/blue/.venv/bin/python", "-m", "package_redis_operator_blue.controller", "--in-cluster"]
CMD ["colors-dev"]

FROM green AS default
