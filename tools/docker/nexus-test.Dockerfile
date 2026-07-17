FROM node:24-bookworm-slim AS node-runtime

FROM rust:1-bookworm

COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=node-runtime /usr/local/lib/node_modules /usr/local/lib/node_modules

RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        jq \
        libssl-dev \
        netcat-openbsd \
        pkg-config \
        procps \
        python3 \
        python3-pip \
        ripgrep \
        socat \
        sqlite3 \
        tmux \
        xz-utils \
        zsh \
    && npm install -g pnpm@10 \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_TARGET_DIR=/tmp/nexus-target
ENV NEXUS_NO_AUTOSTART=1
ENV NEXUS_HOME=/tmp/nexus-home
ENV NEXUS_DB_PATH=/tmp/nexus-home/nexus.db
ENV NEXUS_STREAM_DB_PATH=/dev/shm/nexus-stream.db
ENV NEXUS_SOCKET_PATH=/tmp/nexus-home/nexus.sock
ENV NEXUS_EDGE_BIND=0.0.0.0:4100
ENV NEXUS_GATEWAY_BIND=127.0.0.1
ENV NEXUS_GATEWAY_PORT=4101
ENV HOME=/tmp/harness-home
ENV CODEX_HOME=/tmp/harness-home/.codex
ENV CLAUDE_CONFIG_DIR=/tmp/harness-home/.claude
ENV HERMES_HOME=/tmp/harness-home/.hermes
ENV NEXUS_OPENCODE_HOME=/tmp/harness-home/.opencode-nexus
ENV OPENCODE_DB=/tmp/harness-home/.opencode-nexus/opencode.db
ENV XDG_RUNTIME_DIR=/tmp/runtime

# portable-pty uses HOME as the child cwd when a CommandBuilder has no explicit cwd. Keep the
# declared test HOME valid and writable by the host UID used for OAuth harness validation, even for
# direct `docker run ... cargo test` invocations that do not pass through the runtime setup.
RUN install -d -m 0777 "$HOME"

WORKDIR /work/egregore-nexus

CMD ["bash"]
