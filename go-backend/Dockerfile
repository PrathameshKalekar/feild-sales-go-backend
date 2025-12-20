FROM golang:1.22-alpine

WORKDIR /app

# Install CompileDaemon
RUN go install github.com/githubnemo/CompileDaemon@latest

# Copy go.mod first (for caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy rest of the code
COPY . .

EXPOSE 8080

CMD ["CompileDaemon","--build=go build -o app","--command=./app","--exclude-dir=.git"]
