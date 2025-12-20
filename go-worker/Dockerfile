FROM golang:1.22-alpine

WORKDIR /app

RUN go install github.com/githubnemo/CompileDaemon@latest

COPY go.mod go.sum ./
RUN go mod download

COPY . .

CMD ["CompileDaemon","--build=go build -o worker","--command=./worker","--exclude-dir=.git"]
