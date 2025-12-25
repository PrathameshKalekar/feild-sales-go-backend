FROM golang:1.23-alpine

WORKDIR /app

RUN go install github.com/githubnemo/CompileDaemon@latest

# Copy go.mod and go.sum first
COPY go.mod go.sum ./
RUN go mod download

# Copy all project files
COPY go-worker ./go-worker
COPY internal ./internal
COPY .env* ./

WORKDIR /app

# Use CompileDaemon for live-reloading
CMD ["CompileDaemon", "--build=go build -o /worker ./go-worker/main.go", "--command=/worker", "--exclude-dir=.git"]
