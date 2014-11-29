package sstable;

/**
 * Writes one shard of sstable. sstable is sorted by keys. Once
 * written sstables are immutable. sstable writer will aggregate
 * data in memory and will flush when large amount of data is 
 * accumulated. Writes are done to a temporary location.
 * before closing the file all sstables are merged.
 * Concrete implementation will be separate for Hadoop and local.
 * TODO(Nikunj): T extends Thrift
 */
public abstract class SSTableWriter<T> {
  /**
   * Adds record to SSTable. 'key' doesnt have to be unique.
   * write doesn't succeed unless it gets flushed. 
   */
  public void write(String key, T value);

  /**
   * Commits write. Flush all the data to persistant disk.
   * Once flushed the data will not be lost and can be recovered.
   * Flush will perform merge sort with the existing data and
   * data in buffer. It will also update indices for the shard.
   */
  public void flush();

  /**
   * Closes sstable. No further modification can happen to the file.
   */
  public void close();

}