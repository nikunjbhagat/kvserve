package kvserve;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import java.io.Serializable;

public class DiskSSTableWriter<T extends Serializable> extends SSTableWriter<T> {
  
  private ConcurrentSkipListMap<String, T> buffer;
  private int bufferSize;
  private int currentChunk;
  private ArrayList<String> chunks;
  private final String name;
  private final int maxBufferSize;
  
  /**
   * Create a SSTable file on disk. All data will be written in
   * single file.
   */
  public DiskSSTableWriter(String name, int maxBufferSize) {
    this.name = name;
    this.maxBufferSize = maxBufferSize;
    bufferSize = 0;
    currentChunk = 0;
    chunks = new ArrayList<String>();
    buffer = new ConcurrentSkipListMap<String, T>();
  }

  /**
   * Puts <key, value> in buffer and if buffer gets full then
   * flushes it to disk.
   */
  public void write(String key, T value) {
    buffer.put(key, value);
    bufferSize++;
    if (needFlush()) {
      flush();
    }
  }

  public void flush() {
    ConcurrentSkipListMap<String, T> flushedBuffer;
    int flushedChunk;
    synchronized(this) {
      flushedBuffer = buffer;
      flushedChunk = currentChunk;
      buffer = new ConcurrentSkipListMap<String, T>();
      currentChunk++;
    }
    String flushedFilename = name + ".part" + String.format("%05d", flushedChunk);
    DiskUtils.writeCollectionToFile(flushedFilename, flushedBuffer.entrySet());
  }

  public void close() {
    flush();
    DiskUtils.mergeSortedFile(chunks, name, maxBufferSize);
  }

  /**
   * If buffer size exceeds allowed size, then merge current
   * buffer contents to disk.
   */
  private boolean needFlush() {
    return bufferSize > maxBufferSize;
  }
}