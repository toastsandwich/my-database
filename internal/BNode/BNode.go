package bnode

import (
	"encoding/binary"

	"github.com/toastsandwich/create-database/consts"
	"github.com/toastsandwich/create-database/utils"
)

/*
BNode Struct:
Data: This field represents the actual data stored within the node. In this implementation, it's represented as a byte slice ([]byte). The data can be accessed or manipulated through methods defined on this struct.

Methods:

BType() uint16:
Purpose: This method retrieves the B-tree node's type from its header.
Implementation: It reads a 16-bit unsigned integer (uint16) from the beginning of the Data slice (assuming it's stored in little-endian format) and returns it.

NKeys() uint16:
Purpose: This method retrieves the number of keys stored in the node from its header.
Implementation: It reads a 16-bit unsigned integer (uint16) from the 3rd and 4th bytes of the Data slice (again assuming little-endian format) and returns it.

SetHeader(btype, nkeys uint16):
Purpose: This method sets the header of the B-tree node, including its type and the number of keys.
Implementation: It writes the provided B-tree type and number of keys as 16-bit unsigned integers into the first two bytes of the Data slice.

GetPtr(idx uint16) uint64:
Purpose: This method retrieves the pointer stored at a specific index within the node.
Implementation: It calculates the position of the pointer within the Data slice based on the index and reads a 64-bit unsigned integer (uint64) from that position, assuming it's stored in little-endian format.

SetPtr(idx uint16, val uint64):
Purpose: This method sets the pointer at a specific index within the node to the provided value.
Implementation: It calculates the position of the pointer within the Data slice based on the index and writes the provided 64-bit unsigned integer value into that position, assuming little-endian format.
*/

type BNode struct {
	Data []byte // can be used to dump data into secondary memory
}

// Decoding BTREE NODE
// node is just an array of bytes so adding sime helper funcitons to access its content

// header
// This method retrieves the B-tree node's type from its header.
func (n *BNode) BType() uint16 {
	return binary.LittleEndian.Uint16(n.Data)
}

// This method retrieves the number of keys stored in the node from its header.
func (n *BNode) NKeys() uint16 {
	return binary.LittleEndian.Uint16(n.Data[2:4])
}

// This method sets the header of the B-tree node, including its type and the number of keys.
func (n *BNode) SetHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(n.Data[0:2], btype)
	binary.LittleEndian.PutUint16(n.Data, nkeys)
}

// pointer

// This method retrieves the pointer stored at a specific index within the node.
func (n *BNode) GetPtr(idx uint16) uint64 {
	utils.Assert(idx < n.NKeys())
	pos := consts.HEADER + 8*idx
	return binary.LittleEndian.Uint64(n.Data[pos:])
}

// This method sets the pointer at a specific index within the node to the provided value.
func (n *BNode) SetPtr(idx uint16, val uint64) {
	utils.Assert(idx < n.NKeys())
	pos := consts.HEADER + 8*idx
	binary.LittleEndian.PutUint64(n.Data[pos:], val)
}

/*
Some details about the offset list:
• The offset is relative to the position of the first KV pair.
• The offset of the first KV pair is always zero, so it is not stored in the list.
• We store the offset to the end of the last KV pair in the offset list, which is used to
determine the size of the node.
*/

// offset list

// Calculates the byte offset within the node's Data slice where the offset for a specific key-value pair is stored.
func OffsetPos(n *BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= n.NKeys())
	return consts.HEADER + 8*n.NKeys() + 2*(idx-1)
}

// Retrieves the offset for a specific key-value pair within the node.
func (n *BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(n.Data[OffsetPos(n, idx):])
}

// Sets the offset for a specific key-value pair within the node.
func (n *BNode) SetOffset(idx, offset uint16) {
	binary.LittleEndian.PutUint16(n.Data[OffsetPos(n, idx):], offset)
}

// The offset list is used to locate the nth KV pair quickly

// key-values

// Calculates the byte position within the node's Data slice where a specific key-value pair is stored.
func (n *BNode) KVPos(idx uint16) uint16 {
	utils.Assert(idx <= n.NKeys())
	return consts.HEADER + 8*n.NKeys() + 2*n.NKeys() + n.GetOffset(idx)
}

// Retrieves the key associated with a specific index within the node
func (n *BNode) GetKey(idx uint16) []byte {
	utils.Assert(idx <= n.NKeys())
	pos := n.KVPos(idx)
	klen := binary.LittleEndian.Uint16(n.Data[pos:])
	return n.Data[pos+4:][:klen]
}

// Retrieves the value associated with a specific index within the node.
func (n *BNode) GetVal(idx uint16) []byte {
	utils.Assert(idx <= n.NKeys())
	pos := n.KVPos(idx)
	klen := binary.LittleEndian.Uint16(n.Data[pos+0:])
	vlen := binary.LittleEndian.Uint16(n.Data[pos+2:])
	return n.Data[pos+4+klen:][:vlen]
}

// determinze the size of the node
func (n *BNode) Nbyte() uint16 {
	return n.KVPos(n.NKeys())
}

// This lookup wil ignore first key since it has already compared from parent node.
// works for both internal and leaf node.

//-------------------------------

// add a new key to a leaf node
func LeafInsert(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.SetHeader(consts.BNODE_BLEAF, old.NKeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.NKeys()-idx)
}

//nodeAppendRange function copies keys from an old node to a new node

// copy multiple KVs into the poition
func NodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	utils.Assert(srcOld+n <= old.NKeys())
	utils.Assert(dstNew+n <= new.NKeys())
	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.SetPtr(dstNew+i, old.GetPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.GetOffset(dstNew)
	srcBegin := old.GetOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.GetOffset(srcOld+i) - srcBegin
		new.SetOffset(dstNew+i, offset)
	}

	//KVs
	begin := old.KVPos(srcOld)
	end := old.KVPos(srcOld + n)
	copy(new.Data[new.KVPos(dstNew):], old.Data[begin:end])
}

func NodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.SetPtr(idx, ptr)
	// KVs
	pos := new.KVPos(idx)
	binary.LittleEndian.PutUint16(new.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.Data[pos+2:], uint16(len(val)))
	copy(new.Data[pos+4:], key)
	copy(new.Data[pos+4+uint16(len(key)):], val)
	// the offset of next key
	new.SetOffset(idx+1, new.GetOffset(idx)+4+uint16(len(key)+len(val)))
}
