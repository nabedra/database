# The pinned TLC and Apalache image for design specs (specs/README.md, "Tools").
#
# Build it through specs/tools/tlc, which reads specs/tools/versions.toml and passes every value
# below as a build argument. There are no defaults here on purpose: versions.toml is the single
# source. Every download is verified against its SHA-256 before it is installed.

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG TLC_VERSION
ARG TLC_URL
ARG TLC_SHA256
ARG CM_VERSION
ARG CM_URL
ARG CM_SHA256
ARG APALACHE_VERSION
ARG APALACHE_URL
ARG APALACHE_SHA256

SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

RUN test -n "$TLC_SHA256" && test -n "$CM_SHA256" && test -n "$APALACHE_SHA256" \
 && mkdir -p /opt/tla /tmp/dl && cd /tmp/dl \
 && curl --fail --silent --show-error --location --retry 3 -o tla2tools.jar "$TLC_URL" \
 && curl --fail --silent --show-error --location --retry 3 -o CommunityModules-deps.jar "$CM_URL" \
 && curl --fail --silent --show-error --location --retry 3 -o apalache.tgz "$APALACHE_URL" \
 && printf '%s  %s\n' "$TLC_SHA256" tla2tools.jar "$CM_SHA256" CommunityModules-deps.jar \
      "$APALACHE_SHA256" apalache.tgz | sha256sum --check --strict \
 && mv tla2tools.jar CommunityModules-deps.jar /opt/tla/ \
 && mkdir -p /opt/apalache && tar -xzf apalache.tgz -C /opt/apalache --strip-components=1 \
 && rm -rf /tmp/dl \
 && printf 'tlc=%s\ncommunity_modules=%s\napalache=%s\n' "$TLC_VERSION" "$CM_VERSION" "$APALACHE_VERSION" \
      > /opt/tla/VERSIONS

# `tlc <args>`: TLC with the CommunityModules on the class path and specs/lib (mounted at
# /specs/lib) on the TLA+ library path. JAVA_TOOL_OPTIONS sets the heap.
RUN printf '%s\n' '#!/bin/sh' \
      'exec java -XX:+UseParallelGC -DTLA-Library=/specs/lib -cp /opt/tla/tla2tools.jar:/opt/tla/CommunityModules-deps.jar tlc2.TLC "$@"' \
      > /usr/local/bin/tlc \
 && printf '%s\n' '#!/bin/sh' 'exec /opt/apalache/bin/apalache-mc "$@"' > /usr/local/bin/apalache-mc \
 && chmod 755 /usr/local/bin/tlc /usr/local/bin/apalache-mc \
 && mkdir -p /etc/nabedra-home/.tlaplus \
 && printf 'common {\n  out-dir = "/tmp/apalache-out"\n}\n' > /etc/nabedra-home/.tlaplus/apalache.cfg \
 && chmod -R a+rX /etc/nabedra-home

# The container runs as the caller's user with specs/ read-only, so every tool writes under /tmp,
# or under /out when the wrapper mounts one.
ENV HOME=/etc/nabedra-home TMPDIR=/tmp
WORKDIR /specs
