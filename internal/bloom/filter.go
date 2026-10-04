package bloom

import "hash/fnv"

type Filter struct {
	bits  []byte
	width uint32
	hashs uint32
}

func New(items, bitsPerItem, hashes int) *Filter {
	if items < 1 {
		items = 1
	}
	if bitsPerItem < 4 {
		bitsPerItem = 10
	}
	if hashes < 1 {
		hashes = 3
	}
	width := uint32(items * bitsPerItem)
	return &Filter{bits: make([]byte, (width+7)/8), width: width, hashs: uint32(hashes)}
}

func (filter *Filter) Add(value string) {
	for index := uint32(0); index < filter.hashs; index++ {
		position := filter.position(value, index)
		filter.bits[position/8] |= 1 << (position % 8)
	}
}
func (filter *Filter) MayContain(value string) bool {
	for index := uint32(0); index < filter.hashs; index++ {
		position := filter.position(value, index)
		if filter.bits[position/8]&(1<<(position%8)) == 0 {
			return false
		}
	}
	return true
}
func (filter *Filter) position(value string, seed uint32) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
	_, _ = hash.Write([]byte(value))
	return hash.Sum32() % filter.width
}
