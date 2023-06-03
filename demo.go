package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"sync"
)

const K = 3

// Define Node struct
type Node struct {
	ID string
}

// Define Bucket struct
type Bucket struct {
	nodes []Node
	size int
}

// Define DHT struct
type DHT struct {
	buckets [160]Bucket
	lock sync.RWMutex
}

// Define Peer struct
type Peer struct {
	ID string
	dht *DHT
}

// Create a new Peer node
func NewPeer(id string) *Peer {
	dht := &DHT{}
	for i := 0; i < 160; i++ {
		dht.buckets[i] = Bucket{}
	}
	return &Peer{
		ID: id,
		dht: dht,
	}
}

// Insert a node into the appropriate bucket
func (p *Peer) InsertNode(nodeId string) {
	p.dht.lock.Lock()
	defer p.dht.lock.Unlock()
	index := p.dht.findBucketIndex(nodeId)
	bucket := &p.dht.buckets[index]

	// Check if bucket is full
	if len(bucket.nodes) < K {
		// Bucket is not full, insert the new node
		bucket.nodes = append(bucket.nodes, Node{ID: nodeId})
	} else {
		// Bucket is full, replace the oldest node or delete it
		oldestNode := &bucket.nodes[0]
		for i := 1; i < K; i++ {
			if oldestNode.ID > bucket.nodes[i].ID {
				oldestNode = &bucket.nodes[i]
			}
		}

		var xorNew, xorOld int
		fmt.Sscanf(nodeId, "%d", &xorNew)
		fmt.Sscanf(oldestNode.ID, "%d", &xorOld)
		distance := xorNew ^ xorOld

		if distance < (1 << uint(index)) {
			*oldestNode = Node{ID: nodeId}
		} else {
			bucket.nodes = append(bucket.nodes[:0], bucket.nodes[1:]...)
			bucket.nodes = append(bucket.nodes, Node{ID: nodeId})
		}
	}
}

// Delete a node from the appropriate bucket
func (p *Peer) DeleteNode(nodeId string) {
	p.dht.lock.Lock()
	defer p.dht.lock.Unlock()
	index := p.dht.findBucketIndex(nodeId)
	bucket := &p.dht.buckets[index]

	for i, node := range bucket.nodes {
		if node.ID == nodeId {
			bucket.nodes = append(bucket.nodes[:i], bucket.nodes[i+1:]...)
			break
		}
	}
}

// Update a node in the appropriate bucket
func (p *Peer) UpdateNode(nodeId string, newNode Node) {
	p.dht.lock.Lock()
	defer p.dht.lock.Unlock()
	index := p.dht.findBucketIndex(nodeId)
	bucket := &p.dht.buckets[index]

	for i, node := range bucket.nodes {
		if node.ID == nodeId {
			bucket.nodes[i] = newNode
			break
		}
	}
}

// Find a node in the appropriate bucket
func (p *Peer) FindNode(nodeId string) bool {
	p.dht.lock.RLock()
	defer p.dht.lock.RUnlock()
	index := p.dht.findBucketIndex(nodeId)
	bucket := &p.dht.buckets[index]

	for _, node := range bucket.nodes {
		if node.ID == nodeId {
			return true
		}
	}
	return false
}

// Print the contents of all buckets in the DHT
func (p *Peer) PrintBucketContents() {
	p.dht.lock.RLock()
	defer p.dht.lock.RUnlock()
	for i, bucket := range p.dht.buckets {
		fmt.Printf("Bucket %d: ", i)
		for _, node := range bucket.nodes {
			fmt.Printf("%s ", node.ID)
		}
		fmt.Println()
	}
}

// Find the index of the appropriate bucket for a given node ID
func (d *DHT) findBucketIndex(nodeId string) int {
	var xor int
	fmt.Sscanf(nodeId, "%d", &xor)

	for i := 159; i >= 0; i-- {
		if (xor & (1 << uint(i))) != 0 {
			return i
		}
	}
	return 0
}



// Get the distance between two node IDs
func (d *DHT) getDistance(nodeId1, nodeId2 string) int {
	var xor1, xor2 int
	fmt.Sscanf(nodeId1, "%d", &xor1)
	fmt.Sscanf(nodeId2, "%d", &xor2)
	return xor1 ^ xor2
}

// Calculate the hash of a given key
func hash(key []byte) []byte {
	h := sha1.New()
	h.Write(key)
	return h.Sum(nil)
}

// SetValue function for a Peer
func (p *Peer) SetValue(key, value []byte) bool {
	// Step 1: Check if Key is the hash of Value
	if !bytes.Equal(key, hash(value)) {
		return false
	}

	// Step 2: Check if current Peer already has the key-value pair
	index := p.dht.findBucketIndex(p.ID)
	bucket := &p.dht.buckets[index]
	for _, node := range bucket.nodes {
		if node.ID == string(key) {
			return true
		}
	}

	// Save the key-value pair in current Peer
	bucket.nodes = append(bucket.nodes, Node{ID: string(key)})

	// Step 3: Find the 2 closest nodes to the key in the bucket and call SetValue on them
	var closestNodes [2]Node
	for _, node := range bucket.nodes {
		if node.ID != p.ID {
			if closestNodes[0].ID == "" {
				closestNodes[0] = node
			} else if closestNodes[1].ID == "" {
				if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[0].ID, string(key)) {
					closestNodes[1] = closestNodes[0]
					closestNodes[0] = node
				} else {
					closestNodes[1] = node
				}
			} else {
				if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[0].ID, string(key)) {
					closestNodes[1] = closestNodes[0]
					closestNodes[0] = node
				} else if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[1].ID, string(key)) {
					closestNodes[1] = node
				}
			}
		}
	}

	for _, node := range closestNodes {
		if node.ID != "" {
			peer := &Peer{ID: node.ID, dht: p.dht}
			peer.SetValue(key, value)
		}
	}

	return true
}

// GetValue function for a Peer
func (p *Peer) GetValue(key []byte) []byte {
	// Check if current Peer has the value for the key
	index := p.dht.findBucketIndex(p.ID)
	bucket := &p.dht.buckets[index]
	for _, node := range bucket.nodes {
		if node.ID == string(key) {
			return key
		}
	}


	// Find the 2 closest nodes to the key and call GetValue on them
	var closestNodes [2]Node
	for _, node := range bucket.nodes {
		if node.ID != p.ID {
			if closestNodes[0].ID == "" {
				closestNodes[0] = node
			} else if closestNodes[1].ID == "" {
				if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[0].ID, string(key)) {
					closestNodes[1] = closestNodes[0]
					closestNodes[0] = node
				} else {
					closestNodes[1] = node
				}
			} else {
				if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[0].ID, string(key)) {
					closestNodes[1] = closestNodes[0]
					closestNodes[0] = node
				} else if p.dht.getDistance(node.ID, string(key)) < p.dht.getDistance(closestNodes[1].ID, string(key)) {
					closestNodes[1] = node
				}
			}
		}
	}

	for _, node := range closestNodes {
		if node.ID != "" {
			peer := &Peer{ID: node.ID, dht: p.dht}
			value := peer.GetValue(key)
			if value != nil && bytes.Equal(key, hash(value)) {
				return value
			}
		}
	}

	return nil
}

func main() {
	// Initialize 100 nodes
	var nodes []*Peer
	for i := 0; i < 100; i++ {
		node := NewPeer(fmt.Sprintf("node%d", i))
		nodes = append(nodes, node)
	}

	// Generate 200 random strings and their hash values
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 200; i++ {
		key := make([]byte, rand.Intn(100)+1)
		rand.Read(key)
		value := make([]byte, rand.Intn(100)+1)
		rand.Read(value)
		keys = append(keys, hash(key))
		values = append(values, value)
	}

	// Randomly select nodes to set the key-value pairs
	var selectedNodes []*Peer
	for _, key := range keys[:] {
		node := nodes[rand.Intn(len(nodes))]
		selectedNodes = append(selectedNodes, node)
		node.SetValue(key, values[rand.Intn(len(values))])
	}
	// Randomly select 100 keys and nodes to perform GetValue
	var selectedKeys [][]byte
	var selectedGetValueNodes []*Peer
	for i := 0; i < 100; i++ {
		key := keys[rand.Intn(len(keys))]
		node := nodes[rand.Intn(len(nodes))]
		selectedKeys = append(selectedKeys, key)
		selectedGetValueNodes = append(selectedGetValueNodes, node)
	}

	// Perform GetValue on the selected keys and nodes
	for i, key := range selectedKeys {
		node := selectedGetValueNodes[i]
		value := node.GetValue(key)
		if value != nil {
			fmt.Printf("Node %s retrieved value %x for key %x\n", node.ID, value, key)
		} else {
			fmt.Printf("Node %s could not retrieve value for key %x\n", node.ID, key)
		}
	}

		// Test SetValue and GetValue functions
		value := []byte("hello")
		key := hash(value)

		// Set the value for the key on peer1
		nodes[1].SetValue(key, value)

		// Get the value for the key on peer2
		result := nodes[1].GetValue(key)
		if result == nil {
			fmt.Println("Value not found for key:", key)
		} else if bytes.Equal(result, hash(value)) {
			fmt.Println("Value found for key:", key)
		} else {
			fmt.Println("Invalid value found for key:",key)
		}
}

//func main() {
//
//	// Create 100 nodes
//	nodes := make([]*Peer, 100)
//	for i := 0; i < 100; i++ {
//		nodes[i] = NewPeer(fmt.Sprintf("node-%d", i))
//	}
//
//
//
//	// Insert peers into the DHT
//	nodes[1].InsertNode("2")
//	nodes[1].InsertNode("10")
//	nodes[1].InsertNode("20")
//	nodes[1].InsertNode("30")
//	nodes[1].InsertNode("40")
//
//	nodes[2].InsertNode("1")
//	nodes[2].InsertNode("7")
//	nodes[2].InsertNode("15")
//	nodes[2].InsertNode("25")
//	nodes[2].InsertNode("35")
//
//	nodes[3].InsertNode("6")
//	nodes[3].InsertNode("14")
//	nodes[3].InsertNode("22")
//	nodes[3].InsertNode("33")
//	nodes[3].InsertNode("43")
//
//	nodes[4].InsertNode("3")
//	nodes[4].InsertNode("8")
//	nodes[4].InsertNode("18")
//	nodes[4].InsertNode("28")
//	nodes[4].InsertNode("38")
//
//	nodes[5].InsertNode("4")
//	nodes[5].InsertNode("12")
//	nodes[5].InsertNode("21")
//	nodes[5].InsertNode("31")
//	nodes[5].InsertNode("41")
//
//	// Print the contents of all buckets in each peer
//	fmt.Println("Peer 1 bucket contents:")
//	nodes[1].PrintBucketContents()
//
//	fmt.Println("Peer 2 bucket contents:")
//	nodes[2].PrintBucketContents()
//
//	fmt.Println("Peer 3 bucket contents:")
//	nodes[3].PrintBucketContents()
//
//	fmt.Println("Peer 4 bucket contents:")
//	nodes[4].PrintBucketContents()
//
//	fmt.Println("Peer 5 bucket contents:")
//	nodes[5].PrintBucketContents()
//
//	// Test SetValue and GetValue functions
//	value := []byte("hello")
//	key := hash(value)
//
//	// Set the value for the key on peer1
//	nodes[1].SetValue(key, value)
//
//	// Get the value for the key on peer2
//	result := nodes[1].GetValue(key)
//	if result == nil {
//		fmt.Println("Value not found for key:", key)
//	} else if bytes.Equal(result, hash(value)) {
//		fmt.Println("Value found for key:", key)
//	} else {
//		fmt.Println("Invalid value found for key:",key)
//	}
//}