package kvserve;

import java.util.Iterator;

/**
 * Simple reader to read/iterate over the contents of a
 * SSTable shard. The reader loads header for this shard
 * in memory.
 * Separate implementation for Hadoop and local sstable.
 * TODO(Nikunj): T extends Thrift.
 */

public abstract class SSTableReader<T> {
  /**
   * TODO(Nikunj): Use com.google.collections.Pair.
   */
  public static class Pair <U, V> {
    public U first;
    public V second;
  }

  /**
   * Read value corresponding to a key. If key doesn't belong
   * to this shard, or it is absent, null will be returned.
   */
  public abstract T read(String key);

  /**
   * Gets iterator for sstable shard.
   */
  public abstract Iterator<Pair<String, T>> iterator();
}