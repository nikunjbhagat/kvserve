package kvserve;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * Utility class for performing disk operations.
 * TODO(nbhagat): Add logging.
 */
public class DiskUtils {
  /**
   * Writes an stream to disk, object by object
   */
  public static <T extends Serializable> void writeCollectionToFile(
      String name, Stream<T> collection) {
    try {
      FileOutputStream chunkFile = new FileOutputStream(name);
      ObjectOutputStream fileStream = new ObjectOutputStream(chunkFile);
      collection.map(fileStream::writeObject);
      fileStream.close();
      chunkFile.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to write chunk. Cause: " + ioe.getMessage());
    }
  }

  /**
   * Merge File containing collection of Map.Entry<U extends Comparable, V> and produce
   * a single file.
   * TODO(nbhagat): Verify the number of records are same to guarantee correctness.
   */
  public static <U extends Comparable, V> Stream<Map.Entry<U, V>> mergeSortedFile(
      ArrayList<String> fileChunks, String outputName, int batchSize) {
    if (fileChunks.isEmpty()) {
      return;
    }
    try {
      if (fileChunks.size() == 1) {
        // Only one file, can be simply renamed.
        new File(fileChunks.get(0)).rename(new File(outputName));
      }
      int recordPerFile = batchSize / fileChunks.size();
      // Read recordPerFile records from each file chunk and merge them in memory and
      // write to outputName.
      ArrayList<BatchedStreamIterator<Map.Entry<U, V>>> fileStream =
          fileChunks.stream()
          .map(FileInputStream::new)
          .map(ObjectInputStream::new)
          .map(ois -> (BatchedStreamIterator<Map.Entry<U, V>>) new BatchedStreamIterator(
                  ois, batchSize))
          .collect(Collectors.toCollection(ArrayList::new));
      PriorityQueue<MergeData<U, V>> mergeHead =
          (PriorityQueue<MergeData<U, V>>) new PriorityQueue();
      for (int i = 0; i < fileStream.size(); ++i) {
        mergeHead.add(new MergeData(fileStream.get(i).next(). i);
      }
      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputName));
      ArrayList<Map.Entry<U, V>> writeBuffer = (ArrayList<Map.Entry<U, V>>) new ArrayList(); 
      while (!mergeHead.isEmpty()) {
        MergeData<U, V> head = mergeHead.poll();
        writeBuffer.add(head.underlying);
        Map.Entry<U, V> entry = fileStream.get(head.index).next();
        if (entry != null) {
          mergeHead.add(new MergeData(entry, head.index));
        }
        if (writeBuffer.size() > batchSize) {
          // Flush content of buffer to disk.
          // TODO(nbhagat): Do in a separate thread.
          writeBuffer.stream().map(oos::writeObject);
          writeBuffer = (ArrayList<Map.Entry<U, V>>) new ArrayList();
        }
      }

      if (!writeBuffer.isEmpty()) {
        writeBuffer.stream().map(oos::writeObject);
      }
      oos.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to merge chunks. Cause: " + ioe.getMessage());
    }
  }

  /**
   * Structure to hold index and data from stream to be merged.
   */
  private static class MergeData<U extends Comparable, V> implements Comparable<MergeData<U, V>>, Serializable {
    public int index;
    public U key;
    public V value;
    public Map.Entry<U, V> underlying;

    public MergeData(Map.Entry<U, V> entry, int index) {
      this.index = index;
      key = entry.getKey();
      value = entry.getValue();
      this.underlying = entry;
    }
    @Override
    /**
     * Comparator returns strict ordering wrt key. For value field, order is not
     * preserved. However, this will return 0, iff key, value and index are all equal.
     * Both key must be non-null. Value may or mayn't be null.
     */
    public int compareTo(MergeData<U, V> obj) {
      if (obj == null) {
        return 1;
      }
      int ret = key.compareTo(obj.key);
      if (ret == 0) {
        ret = value.hashCode() - (obj.value == null ? 0 : obj.value.hashCode());
      }
      if (ret == 0) {
        ret = index - obj.index;
      }
      return ret;
    }

    @Override
    public boolean equals(MergeData<U, V> obj) {
      if (value == null && obj.value != null) {
        return false;
      }
      return key.equals(obj.key) && value.equals(obj.value) && index.equals(obj.index);
    }
  }
}