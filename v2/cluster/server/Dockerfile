# Dockerfile
FROM golang:alpine

WORKDIR /app

COPY . .

RUN go build -o server ./server.go


CMD ["sh", "-c", "./server"]
