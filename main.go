package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/mana-sg/StreamDB/pkg/ipfs"
	shell "github.com/ipfs/go-ipfs-api"
)

func main() {
	// Create a new cluster with 1MB chunk size (smaller chunks for testing)
	cluster := ipfs.NewCluster(256 * 1024) // 256KB chunks for testing

	// Add IPFS nodes to the cluster - try different ports if you have multiple nodes
	nodes := []string{
		"localhost:5001", // Primary node
	}

	// Add more nodes if you have them
	additionalNodes := []string{
		"localhost:5002",
		"localhost:5003",
	}

	// First add the main node
	for _, addr := range nodes {
		if err := cluster.AddNode(addr); err != nil {
			log.Fatalf("Failed to add primary node %s: %v", addr, err)
		}
	}

	// Try to add additional nodes but don't fail if they're not available
	for _, addr := range additionalNodes {
		if err := cluster.AddNode(addr); err != nil {
			log.Printf("Note: Additional node %s not available: %v", addr, err)
		}
	}

	// Check if we have at least one node
	availableNodes := cluster.GetAvailableNodes()
	if len(availableNodes) == 0 {
		log.Fatalf("No IPFS nodes available. Please start at least one IPFS daemon.")
	}
	
	log.Printf("Working with %d available nodes: %v", len(availableNodes), availableNodes)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start heartbeat monitor
	go cluster.StartHeartbeatMonitor(ctx)

	// Handle interrupts
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Example: Store a file
	if len(os.Args) > 1 {
		filePath := os.Args[1]

		fmt.Printf("Storing file: %s\n", filePath)
		
		// Split file into chunks
		chunks, err := cluster.CM.SplitFile(filePath)
		if err != nil {
			log.Fatalf("Failed to split file: %v", err)
		}

		// Create a channel to track chunk storage progress
		progressCh := make(chan ipfs.StreamCh, len(chunks))
		var wg sync.WaitGroup

		// Store each chunk on a different node
		for i, chunk := range chunks {
			wg.Add(1)
			go func(idx int, ch *ipfs.Chunk) {
				defer wg.Done()

				// Get least loaded node
				nodeAddr, err := cluster.GetLeastLoadedNode()
				if err != nil {
					progressCh <- ipfs.StreamCh{
						Event: "error",
						Error: fmt.Errorf("no available nodes for chunk %d: %v", idx, err),
					}
					return
				}

				// Increment node load
				cluster.IncrementNodeLoad(nodeAddr)
				defer cluster.DecrementNodeLoad(nodeAddr)

				// Get node and store chunk
				node := cluster.Nodes[nodeAddr]
				
				fmt.Printf("Storing chunk %d on node %s...\n", idx, nodeAddr)
				
				cidStr, err := node.StoreChunk(ctx, ch)
				if err != nil {
					progressCh <- ipfs.StreamCh{
						Event: "error",
						Error: fmt.Errorf("failed to store chunk %d: %v", idx, err),
					}
					return
				}

				ch.CID = cidStr
				progressCh <- ipfs.StreamCh{
					Event: "progress",
					Data: map[string]interface{}{
						"chunk":    idx,
						"cid":      cidStr,
						"node":     nodeAddr,
						"size":     len(ch.Data),
						"checksum": ch.Checksum[:8] + "...", // Truncated for readability
					},
				}
				
				// Verify chunk is accessible
				_, err = node.RetrieveChunk(ctx, cidStr)
				if err != nil {
					progressCh <- ipfs.StreamCh{
						Event: "warning",
						Error: fmt.Errorf("chunk %d stored but may not be accessible: %v", idx, err),
					}
				}
			}(i, chunk)
		}

		// Monitor progress
		go func() {
			for event := range progressCh {
				if event.Error != nil {
					if event.Event == "warning" {
						log.Printf("Warning: %v", event.Error)
					} else {
						log.Printf("Error: %v", event.Error)
					}
					continue
				}
				fmt.Printf("Progress: %+v\n", event.Data)
			}
		}()

		// Wait for all chunks to be stored
		wg.Wait()
		close(progressCh)

		// Print chunk information
		fmt.Println("\nChunk Information:")
		for _, chunk := range chunks {
			fmt.Printf("Chunk %d: CID=%s, Checksum=%s\n", 
				chunk.Index, chunk.CID, chunk.Checksum)
		}

		// Store the original file in MFS so it appears in WebUI
		node := cluster.Nodes[availableNodes[0]]
		filename := filepath.Base(filePath)
		tempOutputFile := filepath.Join(os.TempDir(), filename)
		
		// Combine chunks back first
		err = cluster.CM.CombineChunks(chunks, tempOutputFile)
		if err != nil {
			log.Printf("Warning: Failed to combine chunks: %v", err)
		} else {
			// Store the combined file in MFS
			file, err := os.Open(tempOutputFile)
			if err != nil {
				log.Printf("Warning: Failed to open temporary file: %v", err)
			} else {
				defer file.Close()
				defer os.Remove(tempOutputFile)
				
				// Use Node.Store method which internally handles MFS storage
				fmt.Printf("Storing complete file in WebUI...\n")
				c, stream, err := node.Store(ctx, tempOutputFile)
				if err != nil {
					log.Printf("Warning: Failed to store file in MFS: %v", err)
				} else {
					for event := range stream {
						if event.Error != nil {
							log.Printf("Error during file storage: %v", event.Error)
						}
					}
					
					// Verify the file is accessible in MFS
					mfsPath := "/my-files/" + filename
					stat, statErr := node.Shell.FilesStat(ctx, mfsPath)
					if statErr != nil {
						log.Printf("Warning: File may not be properly stored in MFS: %v", statErr)
						fmt.Printf("\nFile stored with CID: %s but not verified in MFS\n", c.String())
					} else {
						fmt.Printf("\nFile successfully stored in MFS with CID: %s\n", c.String())
						fmt.Printf("File size in MFS: %d bytes\n", stat.Size)
						fmt.Printf("You can access it in the IPFS WebUI under Files tab at path: %s\n", mfsPath)
					}
					
					// Also create a dedicated chunks directory for this file
					chunksPath := "/my-files/chunks-" + filename
					err = node.Shell.FilesMkdir(ctx, chunksPath, shell.FilesMkdir.Parents(true))
					if err != nil {
						log.Printf("Warning: Failed to create chunks directory in MFS: %v", err)
					} else {
						// Create a manifest file with chunk information
						manifestContent := fmt.Sprintf("Manifest for %s\n\n", filename)
						manifestContent += fmt.Sprintf("Total chunks: %d\n", len(chunks))
						manifestContent += fmt.Sprintf("Original file CID: %s\n\n", c.String())
						manifestContent += "Chunk Details:\n"
						
						// Create references to chunks by copying them to the file-specific chunks directory
						for _, chunk := range chunks {
							// Skip if CID is empty
							if chunk.CID == "" {
								continue
							}
							
							// Add to manifest
							manifestContent += fmt.Sprintf("Chunk %d: CID=%s, Size=%d bytes, Checksum=%s\n", 
								chunk.Index, chunk.CID, len(chunk.Data), chunk.Checksum)
							
							// Get chunk data
							chunkData, err := node.RetrieveChunk(ctx, chunk.CID)
							if err != nil {
								log.Printf("Warning: Failed to retrieve chunk %d for copying: %v", chunk.Index, err)
								continue
							}
							
							// Create a new file in the chunks directory
							chunkReader := bytes.NewReader(chunkData)
							dstPath := fmt.Sprintf("%s/chunk_%d", chunksPath, chunk.Index)
							
							err = node.Shell.FilesWrite(ctx, dstPath, chunkReader, 
								shell.FilesWrite.Create(true), 
								shell.FilesWrite.Parents(true))
							
							if err != nil {
								log.Printf("Warning: Failed to create reference for chunk %d: %v", chunk.Index, err)
							}
						}
						
						// Write the manifest
						manifestReader := bytes.NewReader([]byte(manifestContent))
						manifestPath := fmt.Sprintf("%s/_MANIFEST.txt", chunksPath)
						err = node.Shell.FilesWrite(ctx, manifestPath, manifestReader,
							shell.FilesWrite.Create(true),
							shell.FilesWrite.Parents(true))
						
						if err != nil {
							log.Printf("Warning: Failed to create manifest: %v", err)
						}
						
						fmt.Printf("\nChunks stored in directory: %s\n", chunksPath)
						fmt.Printf("Access chunks via IPFS gateway: http://localhost:8080/ipfs/%s\n", c.String())
						fmt.Printf("Or via WebUI Files section at: %s\n", chunksPath)
						fmt.Printf("Each chunk can be accessed individually by its CID via gateway: http://localhost:8080/ipfs/[CHUNK_CID]\n")
					}
				}
			}
		}

		// Example: Retrieve a chunk
		if len(chunks) > 0 {
			fmt.Println("\nRetrieving first chunk as example:")
			nodeAddr, err := cluster.GetLeastLoadedNode()
			if err != nil {
				log.Printf("Warning: Failed to get node for retrieval: %v", err)
			} else {
				node := cluster.Nodes[nodeAddr]
				data, err := node.RetrieveChunk(ctx, chunks[0].CID)
				if err != nil {
					log.Printf("Warning: Failed to retrieve chunk: %v", err)
				} else {
					fmt.Printf("Retrieved chunk data length: %d bytes\n", len(data))
				}
			}
		}
	} else {
		fmt.Println("Usage: go run main.go <file_path>")
	}
}