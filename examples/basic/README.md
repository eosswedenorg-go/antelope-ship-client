# Basic example using the client

## Configuration

### Ship node

The code expects a nodeos with `eosio::state_history_plugin` enabled to be running on `127.0.0.1:8089`

Edit the followin variable in `main.go` if your state history node is running elsewhere.

```go
// IP and port to the ship node.
var shipHost string = "ws://127.0.0.1:8089"
```

### API Node

It's also recommended that to set `APIURL` to a nodeos server with `chain_api` plugin enabled. like `http://api.eossweden.org` for example.

This can also be the same node that is running the state history plugin.

edit the variable in `main.go`

```go
// Url to the antelope api on the same node as ship is running.
// Use this to fetch a sane value for `startBlock`
var APIURL string
```

## Running the example

To run the example simply execute:

```
go run main.go
```