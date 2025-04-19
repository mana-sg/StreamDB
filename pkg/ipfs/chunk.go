package ipfs

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

const (
	// DefaultChunkSize is the size of each chunk in bytes
	DefaultChunkSize = 1024 * 1024 // 1MB
)

// Chunk represents a portion of a file
type Chunk struct {
	Index    int
	Data     []byte
	CID      string
	Checksum string
}

// ChunkManager handles file chunking and chunk operations
type ChunkManager struct {
	chunkSize int
}

// NewChunkManager creates a new ChunkManager instance
func NewChunkManager(chunkSize int) *ChunkManager {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &ChunkManager{
		chunkSize: chunkSize,
	}
}

// SplitFile splits a file into chunks
func (cm *ChunkManager) SplitFile(filePath string) ([]*Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	fileSize := fileInfo.Size()
	numChunks := int(math.Ceil(float64(fileSize) / float64(cm.chunkSize)))
	chunks := make([]*Chunk, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSize := cm.chunkSize
		if i == numChunks-1 {
			chunkSize = int(fileSize) - (i * cm.chunkSize)
		}

		// Ensure positive chunk size
		if chunkSize <= 0 {
			continue
		}

		// Seek to correct position
		_, err := file.Seek(int64(i*cm.chunkSize), 0)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to chunk %d: %v", i, err)
		}

		chunkData := make([]byte, chunkSize)
		bytesRead, err := io.ReadFull(file, chunkData)
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("failed to read chunk %d: %v", i, err)
		}

		// Adjust the chunk data if we didn't read the full amount
		if bytesRead != chunkSize {
			chunkData = chunkData[:bytesRead]
		}

		checksum := sha256.Sum256(chunkData)
		chunks[i] = &Chunk{
			Index:    i,
			Data:     chunkData,
			Checksum: fmt.Sprintf("%x", checksum),
		}
	}

	return chunks, nil
}

// CombineChunks combines chunks back into a file
func (cm *ChunkManager) CombineChunks(chunks []*Chunk, outputPath string) error {
	if len(chunks) == 0 {
		return fmt.Errorf("no chunks to combine")
	}

	// Sort chunks by index to ensure correct order
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	for i, chunk := range chunks {
		if chunk == nil {
			return fmt.Errorf("chunk %d is nil", i)
		}

		if len(chunk.Data) == 0 {
			return fmt.Errorf("chunk %d has no data", i)
		}

		checksum := sha256.Sum256(chunk.Data)
		if fmt.Sprintf("%x", checksum) != chunk.Checksum {
			return fmt.Errorf("checksum mismatch for chunk %d", chunk.Index)
		}

		_, err := file.Write(chunk.Data)
		if err != nil {
			return fmt.Errorf("failed to write chunk %d: %v", chunk.Index, err)
		}
	}

	return nil
} 