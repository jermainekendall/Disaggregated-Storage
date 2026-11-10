package main

import (
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func peerIDOf(k crypto.PrivKey) (string, error) {
	id, err := peer.IDFromPrivateKey(k)
	return id.String(), err
}
