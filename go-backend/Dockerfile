FROM golang:1.23-alpine

WORKDIR /app

# Install CompileDaemon
RUN go install github.com/githubnemo/CompileDaemon@latest

# Copy go.mod and go.sum from project root
COPY go.mod go.sum ./
RUN go mod download

# Copy project files
COPY go-backend ./go-backend
COPY internal ./internal
COPY .env* ./

WORKDIR /app

EXPOSE 1906

CMD ["CompileDaemon","--build=go build -o /backend ./go-backend/main.go","--command=/backend","--exclude-dir=.git"]
