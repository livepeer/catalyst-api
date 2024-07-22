FROM	golang:1-bullseye	as	gobuild

ARG TARGETARCH

WORKDIR	/src

ADD	go.mod go.sum	./
RUN	go mod download

ADD	.	.
RUN make build

ARG	GIT_VERSION
ENV	GIT_VERSION="${GIT_VERSION}"

FROM	ubuntu:22.04	AS	catalyst

ENV	DEBIAN_FRONTEND=noninteractive

LABEL	maintainer="Amritanshu Varshney <amritanshu+github@livepeer.org>"

ARG	BUILD_TARGET

RUN	apt update && apt install -yqq wget software-properties-common

RUN	apt update && apt install -yqq \
	curl \
	ca-certificates \
	procps \
	vnstat \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=gobuild		/src/build/catalyst-api /bin/catalyst-api

CMD ["/bin/catalyst-api"]