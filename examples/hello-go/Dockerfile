FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o /hello .

FROM scratch
COPY --from=build /hello /hello
CMD ["/hello"]
