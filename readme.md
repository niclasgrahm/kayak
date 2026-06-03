# streamer

## inputs

nats
dummy
streamer

## transforms

http
splitter
reduce
buffer

## outputs

file
nats
stdout
