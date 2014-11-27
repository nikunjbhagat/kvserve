package sstable;

/**
 * Writes one shard of sstable. sstable is sorted by keys. Once
 * written sstables are immutable. sstable writer will aggregate
 * data in memory and will flush when large amount of data is 
 * accumulated. Writes are done to a temporary location.
 * before closing the file all sstables are merged.
 */
public class SSTableWriter {

}