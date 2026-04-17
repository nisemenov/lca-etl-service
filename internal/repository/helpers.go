package repository

const SQLParamsLimit = 500

func chunkItems[T any](items []T, size int) [][]T {
	var chunks [][]T
	for size < len(items) {
		items, chunks = items[size:], append(chunks, items[0:size:size])
	}
	chunks = append(chunks, items)
	return chunks
}
