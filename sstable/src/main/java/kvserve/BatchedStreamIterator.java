package kvserve;

import java.io.ObjectInputStream;
import java.util.Comparable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Create a batched stream backed by a ObjectInputStream. Not thread-safe.
 */
public class BatchedStreamIterator<T> implements Iterator<T> {
  private ObjectInputStream inputStream;
  private LinkedList<T> buffer;
  private boolean isAvailable;
  private final int batchSize;

  /**
   * Creates an iterator over inputStream which will read data in batches.
   * inputStream is assumed to be readonly and no modification is supposed
   * to happen.
   */
  public BatchedStreamIterator(
      ObjectInputStream inputStream, int batchSize) {
    this.buffer = (ArrayList<T>) new ArrayList();
    this.batchSize = batchSize;
    this.inputStream = inputStream;
    this.isAvailable = true;
  }

  @Override
  /**
   * Return next item or null, if stream is empty.
   */
  public T next() {
    fillBuffer();
    if (!buffer.isEmpty()) {
      return buffer.remove()
    }
    return null;
  }

  @Override
  /**
   * Checks if the buffer has data. If not check if inputStream can be read.
   * If buffer is empty and inputStream has data, new data will be pulled
   * into the buffer. If inputStream is empty and no data can be read then,
   * it will close the mark the stream as unavailable.
   */
  public boolean hasNext() {
    fillBuffer();
    return !buffer.isEmpty();
  }

  @Override
  /**
   * Remove is not supported.
   */
  public void remove() {
    return;
  }

  /**
   * Fills buffer if it is empty. If buffer can't be filled then mark
   * stream unavailable. Only function which read directly from inputStream.
   */
  private void fillBuffer() throws IOException {
    if (!buffer.isEmpty() || !isAvailable) {
      return;
    }
    for (int i = 0; i < batchSize && isAvailable; ++i) {
      T obj = (T) inputStream.readObject();
      if (obj != null) {
        buffer.add(obj);
      } else {
        isAvailable = false;
      }
    }
  }
}