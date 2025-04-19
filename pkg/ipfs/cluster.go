package ipfs

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NodeStatus represents the status of an IPFS node
type NodeStatus struct {
	Address       string
	IsAvailable   bool
	LastHeartbeat time.Time
	Load          int // Number of active operations
}

// Cluster manages multiple IPFS nodes
type Cluster struct {
	Nodes    map[string]*Node
	statuses map[string]*NodeStatus
	mu       sync.RWMutex
	CM       *ChunkManager
}

// NewCluster creates a new IPFS node cluster
func NewCluster(chunkSize int) *Cluster {
	return &Cluster{
		Nodes:    make(map[string]*Node),
		statuses: make(map[string]*NodeStatus),
		CM:       NewChunkManager(chunkSize),
	}
}

// AddNode adds a new IPFS node to the cluster
func (c *Cluster) AddNode(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.Nodes[addr]; exists {
		return fmt.Errorf("node already exists: %s", addr)
	}

	node, err := NewNode(addr)
	if err != nil {
		return err
	}

	c.Nodes[addr] = node
	c.statuses[addr] = &NodeStatus{
		Address:       addr,
		IsAvailable:   true,
		LastHeartbeat: time.Now(),
		Load:          0,
	}

	return nil
}

// RemoveNode removes a node from the cluster
func (c *Cluster) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.Nodes, addr)
	delete(c.statuses, addr)
}

// StartHeartbeatMonitor starts monitoring node health
func (c *Cluster) StartHeartbeatMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkNodeHealth()
		}
	}
}

func (c *Cluster) checkNodeHealth() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, status := range c.statuses {
		node := c.Nodes[addr]
		if node == nil {
			continue
		}

		status.IsAvailable = node.IsAvailable()
		if status.IsAvailable {
			status.LastHeartbeat = time.Now()
		}
	}
}

// GetAvailableNodes returns a list of available nodes
func (c *Cluster) GetAvailableNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var available []string
	for addr, status := range c.statuses {
		if status.IsAvailable {
			available = append(available, addr)
		}
	}
	return available
}

// GetLeastLoadedNode returns the address of the least loaded available node
func (c *Cluster) GetLeastLoadedNode() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var minLoad = -1
	var selectedAddr string

	for addr, status := range c.statuses {
		if !status.IsAvailable {
			continue
		}
		if minLoad == -1 || status.Load < minLoad {
			minLoad = status.Load
			selectedAddr = addr
		}
	}

	if selectedAddr == "" {
		return "", fmt.Errorf("no available nodes")
	}

	return selectedAddr, nil
}

// IncrementNodeLoad increases the load counter for a node
func (c *Cluster) IncrementNodeLoad(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if status, exists := c.statuses[addr]; exists {
		status.Load++
	}
}

// DecrementNodeLoad decreases the load counter for a node
func (c *Cluster) DecrementNodeLoad(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if status, exists := c.statuses[addr]; exists {
		status.Load--
	}
} 