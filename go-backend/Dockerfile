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

WORKDIR /app

EXPOSE 1906

CMD ["CompileDaemon","--build=go build -o go-backend/app ./go-backend/main.go","--command=./go-backend/app","--exclude-dir=.git"]
