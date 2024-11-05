ARG GO_VERSION="1.22"
ARG ALPINE_VERSION="3.20"

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ENV GOPROXY="http://xxxx/"
ENV GO111MODULE="on"
ADD . /src
WORKDIR /src
RUN go mod download && \
    go build .

FROM alpine:${ALPINE_VERSION}
ENV TZ Asia/Taipei

COPY --from=builder /src/estool /usr/local/bin/estool
COPY . .
WORKDIR /
CMD ["/usr/local/bin/estool"]