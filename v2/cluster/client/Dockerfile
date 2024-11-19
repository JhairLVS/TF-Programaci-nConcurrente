# Dockerfile
FROM golang:alpine

WORKDIR /app

COPY . . 
RUN go build -o client ./client.go

CMD ["sh", "-c", "./server"]
