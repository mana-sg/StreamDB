# StreamDB

A real-time database that stores data in IPFS nodes and uses smart contracts to handle duplications. We use streams to share data between nodes.

## Quick Start for ipfs

1. Run IPFS daemon:
```
ipfs daemon
```

2. Run the main application with your file:
```
go run main.go <file_path>
```

3. Access the UI at:
```
http://127.0.0.1:5001/webui
```

## Storage

Files and chunks are stored in the `my-files` folder.