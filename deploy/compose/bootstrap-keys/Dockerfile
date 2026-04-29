FROM alpine:3.22

RUN apk add --no-cache openssl

COPY bootstrap-keys.sh /usr/local/bin/bootstrap-keys
RUN chmod +x /usr/local/bin/bootstrap-keys

ENTRYPOINT ["/usr/local/bin/bootstrap-keys"]
