FROM golang:1.20 as builder
WORKDIR /home
RUN git clone --depth 1 https://github.com/golang/go.git
RUN cd go/src \
    && apt-get update && apt-get install -y bzip2 \
    && GOEXPERIMENT=cacheprog GOOS=linux GOARCH=amd64 ./bootstrap.bash
FROM debian as runtime
COPY --from=builder /home/go-linux-amd64-bootstrap/ /usr/local/go
RUN apt-get update && apt-get install -y ca-certificates
ENV PATH=/go/bin:/usr/local/go/bin:$PATH