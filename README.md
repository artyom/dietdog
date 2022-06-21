# dietdog, a bare-bones DataDog logger

Stream logs from the standard library log.Logger to DataDog logs API.

## Why?

I wanted to send logs of an existing Go program to the DataDog,
but the official DataDog Go client is enormous, over 9 Mb of source code:

    $ tar tvzf datadog-api-client-go-1.14.0.tar.gz | \
        awk '$NF~/\.go$/&&$NF!~/_test\.go$/{s+=$5}END{print s}'
    9646862

Yes, I know about the datadog-agent.
It requires more resources than the service that emits logs.
