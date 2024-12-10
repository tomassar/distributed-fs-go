
# P2P Distributed File Storage system
This project implements a distributed file storage system using Go. It utilizes a peer-to-peer network for communication and employs encryption for secure data transfer. The system allows storing, retrieving, and deleting files across multiple nodes in the network.
## Features
- TCP connection between Peers in the P2P network
- Store and hash files locally and remotely
- Secure data transfer using encryption.
- Server to handle file storage and broadcast them
- Gossip protocol
- Encryption/Decryption
- Sync own files from peers
- Network bootstrapping.
## Key Learnings
- Understanding and implementing peer-to-peer communication in Go.
- Working with encryption and decryption for secure data transfer.
- Managing file storage and retrieval across multiple nodes
- Utilizing context for managing request-specific data across functions
## TODO

- Create a graphical/command line interface to use the file storage system
