FROM python:3.12 AS builder

ARG BUILD_VERSION=dev
WORKDIR /build/nodenorm
COPY .. .
RUN echo "${BUILD_VERSION}" > /build/nodenorm/version.txt
RUN pip wheel --wheel-dir=/build/wheels /build/nodenorm

FROM caddy:2.8-builder AS caddy_builder
RUN xcaddy build

FROM python:3.12-slim
RUN apt update -q -y && apt install -y --no-install-recommends git supervisor curl telnet && rm -rf /var/lib/apt/lists/*
RUN useradd -m nodenorm && usermod -aG sudo nodenorm
USER nodenorm
WORKDIR /home/nodenorm/
RUN python -m venv /home/nodenorm/venv
COPY --from=builder --chown=nodenorm:nodenorm /build/wheels /home/nodenorm/wheels
RUN /home/nodenorm/venv/bin/pip install /home/nodenorm/wheels/*.whl && rm -rf /home/nodenorm/wheels

WORKDIR /home/nodenorm/configuration
COPY --from=builder --chown=nodenorm:nodenorm /build/nodenorm/src/nodenorm/application/configuration/default.json /home/nodenorm/configuration
COPY --from=builder --chown=nodenorm:nodenorm /build/nodenorm/version.txt /home/nodenorm/configuration/version.txt

COPY --from=builder --chown=nodenorm:nodenorm /build/nodenorm/docker/configuration/supervisord.conf /etc/supervisor/supervisord.conf
COPY --from=builder --chown=nodenorm:nodenorm /build/nodenorm/docker/configuration/Caddyfile /etc/caddy/Caddyfile

COPY --from=caddy_builder --chown=nodenorm:nodenorm /usr/bin/caddy /usr/bin/caddy


STOPSIGNAL SIGINT
EXPOSE 9000

ENTRYPOINT ["supervisord"]
CMD ["-n", "-c", "/etc/supervisor/supervisord.conf"]
