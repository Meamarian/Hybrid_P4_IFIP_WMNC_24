# Pointers in DPDK Hash Table APIs
The DPDK hash table functions require specific inputs to operate efficiently. Here’s how these inputs are defined and passed to the functions:

## rte_hash_lookup_data
What the function needs:

A pointer to the hash table (struct rte_hash *buffer_table) to access the hash table in memory.
The address of the key (&teid) to calculate the hash and locate the value.
A double pointer (void **) to store the output pointer that references the value associated with the key.
How we provide the inputs:

Define a hash table (struct rte_hash *buffer_table) to use in the lookup.
Define a key (uint32_t teid) and pass its address (&teid) to the function.
Define a pointer for the value (struct packet_in_buffer_t *packet_in_bucket) and pass its address as (void **)&packet_in_bucket.
Example:

```bash
int rte_hash_lookup_data	(const struct rte_hash * h, const void * key, void ** data)	
```
example: 

```bash
struct rte_hash* buffer_table
uint32_t teid = rte_be_to_cpu_32(*(uint32_t *)(pkt_data + 14 + 20 + 8 + 4)); 
struct packet_in_buffer_t* packet_in_bucket;
int ret = rte_hash_lookup_data(buffer_table, &teid, (void **)&packet_in_bucket);
```

## rte_hash_add_key_data

What the function needs:

A pointer to the hash table (struct rte_hash *buffer_table) to modify the table.
The address of the key (&teid) to identify the new entry.
A pointer to the value (void *m) to store in the table.
How we provide the inputs:

Define a key (uint32_t teid) and a value (struct rte_mbuf *m).
Pass the address of the key (&teid) and cast the value pointer to void * when calling the function.
Example:

```bash
struct rte_mbuf * m
int ret = rte_hash_add_key_data(buffer_table, &teid, (void *)m);
```
## rte_hash_del_key

What the function needs:

A pointer to the hash table (struct rte_hash *buffer_table) to locate the table entry.
The address of the key (&teid) to find the entry to delete.
How we provide the inputs:

Define the hash table (struct rte_hash *buffer_table) and the key (uint32_t teid).
Pass the hash table pointer and the address of the key (&teid) to the function.
Example:

```bash
int ret = rte_hash_del_key(buffer_table, &teid);
```
