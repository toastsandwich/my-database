package btree

import (
	"github.com/toastsandwich/create-database/consts"
	bnode "github.com/toastsandwich/create-database/internal/BNode"
	"github.com/toastsandwich/create-database/utils"
)

/*
we can’t use the in-memory pointers, the pointers are 64-bit integers referencing
disk pages instead of in-memory nodes. We’ll add some callbacks to abstract away this
aspect so that our data structure code remains pure data structure code.
*/
type BTree struct {
	Root uint64                   //pointer to non zero page number
	Get  func(uint64) bnode.BNode //derefence a pointer
	New  func(bnode.BNode) uint64 //allocate a new page
	Del  func(uint64)             //deallocate a a page
}

/*
The page size is defined to be 4K bytes. A larger page size such as 8K or 16K also works.
We also add some constraints on the size of the keys and values. So that a node with a
single KV pair always fits on a single page. If you need to support bigger keys or bigger
values, you have to allocate extra pages for them and that adds complexity
*/

func init() {
	nodelmax := consts.HEADER + 8 + 2 + 4 + consts.BTREE_MAX_KEY_SIZE + consts.BTREE_MAX_VAL_SIZE
	utils.Assert(nodelmax <= consts.BTREE_PAGE_SIZE)
}

/*
why is size of btree is determined using this expression ?
=>
In a B-tree data structure, each node typically contains a header, keys, and pointers to child nodes or data. The size of the B-tree node is crucial for optimizing memory usage and performance.

Let's break down the expression and understand why each component is included:

HEADER: The header of the node typically includes metadata such as the number of keys present in the node or other administrative information. This information is essential for navigating and managing the B-tree efficiently.

8 + 2 + 4: These are likely sizes of specific components within the node. For example, 8 might represent the size of a pointer to a child node, 2 could represent the size of a metadata field, and 4 might represent the size of another pointer or metadata field. These sizes are fixed and independent of the actual data stored in the B-tree.

BTREE_MAX_KEY_SIZE and BTREE_MAX_VAL_SIZE: These represent the maximum sizes of keys and values respectively that can be stored in the B-tree. Including these in the calculation ensures that enough space is allocated within the node to accommodate the largest possible key and value pairs.

By summing up these components, the expression calculates the maximum size that a node in the B-tree can occupy. This calculation is crucial for determining memory requirements and optimizing the design of the B-tree implementation to ensure efficient storage and retrieval of data.

*/
