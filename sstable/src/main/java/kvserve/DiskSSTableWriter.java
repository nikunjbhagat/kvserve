package kvserve;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultiMap;
import com.google.common.collect.TreeMultimap;
import java.util.ArrayList;

public class DiskSSTableWriter<T> extends SSTableWriter<T> {
  
  private SortedSetMultiMap<String, T> buffer;
  private int bufferSize;
  private int currentChunk;
  private ArrayList<String> chunks;
  private final String name;
  private final int maxBufferSize;
  
  /**
   * Create a SSTable file on disk. All data will be written in
   * single file.
   */
  public DiskSSTableWriter<T>(
      String name,
      int maxBufferSize) {
    this.name = name;
    this.maxBufferSize = maxBufferSize;
    bufferSize = 0;
    currentChunk = 0;
    chunks = new ArrayList<String>();
    buffer = Multimaps.synchronizedSortedSetMultimap(new TreeMultimap<String, T>());
  }

  /**
   * Puts <key, value> in buffer and if buffer gets full then
   * flushes it to disk.
   */
  public void write(String key, T value) {
    buffer.put(key, value);
    bufferSize++;
  }

  public void flush() {
    SortedSetMultiMap<String, T> flushedBuffer;
    int flushedChunk;
    synchronized(this) {
      flushedBuffer = buffer;
      flushedChunk = currentChunk;
      buffer = Multimaps.synchronizedSortedSetMultimap(new TreeMultimap<String, T>());
      currentChunk++;
    }
    String flushedFilename = name + ".part" + String.format("%05d", flushedChunk);
    DiskUtils.writeCollectionToFile(flushedFilename, flushedBuffer.entries().stream());
  }

  public void close() {
    flush();
    DiskUtils.mergeChunks(chunks, name);
  }

  /**
   * If buffer size exceeds allowed size, then merge current
   * buffer contents to disk.
   */
  private boolean needFlush() {
    if (bufferSize > maxBufferSize) {
      flush();
    }
  }
}