FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /operator .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /operator /operator
ENTRYPOINT ["/operator"]
