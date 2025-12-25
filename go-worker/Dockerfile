FROM golang:1.23-alpine

WORKDIR /app

RUN go install github.com/githubnemo/CompileDaemon@latest

# Copy go.mod and go.sum first
COPY go.mod go.sum ./
RUN go mod download

# Copy all project files
COPY go-worker ./go-worker
COPY internal ./internal

ENV GOPROXY=https://proxy.golang.org,direct

# Build directly - output to /worker (outside volume mount)
WORKDIR /app
RUN go build -o /worker ./go-worker/main.go

CMD ["/worker"]
