FROM	golang:1-bullseye	as	gobuild

ARG TARGETARCH

# Download c2patool needed to sign our C2PA manifest
# We download it from any of our previous builds, because building c2patool from source is very slow with QEMU
RUN	apt update && apt install -yqq \
	curl \
	ca-certificates \
	&& curl https://build.livepeer.live/c2patool/0.6.2/c2patool-linux-${TARGETARCH}.tar.gz -o /c2patool.tar.gz \
	&& tar xzf /c2patool.tar.gz

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
COPY --from=gobuild		/go/c2patool /bin/

CMD ["/bin/catalyst-api"]