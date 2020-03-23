FROM golang:1.14-alpine AS build-env
ENV MYSQL_DATASOURCE=${MYSQL_DATASOURCE}
RUN apk update && apk add curl && apk add git && curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
RUN echo $GOPATH
RUN mkdir /app-conf
ADD /src /go/src/api

COPY firebase.json /app-conf

RUN cd /go/src/api && dep ensure && CGO_ENABLED=0 go get -u github.com/go-swagger/go-swagger/cmd/swagger 
RUN cd /go/src/api && CGO_ENABLED=0 swagger generate spec -m -o ./doc/swagger.json
RUN cd /go/src/api && CGO_ENABLED=0 go test -cover -failfast && go build -o api 

FROM alpine:3.9
RUN apk update && apk add curl
WORKDIR /usr/src
RUN mkdir /usr/src/doc
COPY --from=build-env /go/src/api/api /usr/src
COPY --from=build-env /go/src/api/doc /usr/src/doc
#ENV GIN_MODE=release
RUN echo 'vm.overcommit_memory = 1' >> /etc/sysctl.conf
RUN echo 'vm.swappiness = 1' >> /etc/sysctl.conf
EXPOSE 5000
HEALTHCHECK CMD curl --fail http://localhost:5000/healthz || exit 1
ENTRYPOINT ./api