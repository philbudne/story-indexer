# from https://raw.githubusercontent.com/astral-sh/uv-docker-example/refs/heads/main/multistage.Dockerfile
# An example using multi-stage image builds to create a final image without uv.

# MC changes:
# use python.org images for both stages.

ARG PYTHON_IMAGE=python:3.13-slim-trixie

# First, build the application in the `/app` directory.
# See `Dockerfile` for details.
FROM ${PYTHON_IMAGE} AS builder

# from https://docs.astral.sh/uv/guides/integration/docker/#intermediate-layers
# get uv from astral image (could just mount it for RUN uv .... lines?
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# need git for mc-logging
# used to need gcc for some package install on ARM Macs?
# perform cleanup inside RUN, so files never added to layer
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Omit development dependencies
ENV UV_NO_DEV=1

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

# install indexer in layers after requirements:xx
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

FROM ${PYTHON_IMAGE}

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

# Copy the application from the builder
COPY --from=builder --chown=nonroot:nonroot /app /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Use the non-root user to run our application
USER nonroot

# Use `/app` as the working directory
WORKDIR /app

# pass the executable as ARG and use it in CMD
ARG RUN
ENV RUN=${RUN}

ENTRYPOINT [ "/bin/sh", "-c", "${APP_DOCKER}/bin/indirect.sh" ]
