package kvserve;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.stream.Collectors;
/**
 * Utility class for performing disk operations.
 * TODO(nbhagat): Add logging.
 */
public class DiskUtils {
  /**
   * Writes an stream to disk, object by object
   */
  public static <T extends Serializable> void writeCollectionToFile(
      String name, Collection<Map.Entry<String, T>> collection) {
    try {
      FileOutputStream chunkFile = new FileOutputStream(name);
      ObjectOutputStream oos = new ObjectOutputStream(chunkFile);
      for (Map.Entry<String, T> obj : collection) {
        oos.writeObject(new KVPair<T>(obj.getKey(), obj.getValue()));
      }
      oos.close();
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
  public static <T extends Serializable> void mergeSortedFile(
      ArrayList<String> fileChunks, String outputName, int batchSize) {
    if (fileChunks.isEmpty()) {
      return;
    }
    try {
      if (fileChunks.size() == 1) {
        // Only one file, can be simply renamed.
        new File(fileChunks.get(0)).renameTo(new File(outputName));
        return;
      }
      int recordPerFile = batchSize / fileChunks.size();
      Function<String, BatchedStreamIterator<KVPair<T>>> objStreamGenerator = (String f) -> {
        try {
          return (BatchedStreamIterator<KVPair<T>>) new BatchedStreamIterator(
            new ObjectInputStream(new FileInputStream(f)), batchSize);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      };
      // Read recordPerFile records from each file chunk and merge them in memory and
      // write to outputName.
      ArrayList<BatchedStreamIterator<KVPair<T>>> fileStream =
          fileChunks.stream()
          .map(objStreamGenerator)
          .collect(Collectors.toCollection(ArrayList::new));
      PriorityQueue<MergeData<T>> mergeHead =
          (PriorityQueue<MergeData<T>>) new PriorityQueue();
      for (int i = 0; i < fileStream.size(); ++i) {
        mergeHead.add(new MergeData(fileStream.get(i).next(), i));
      }
      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputName));
      ArrayList<KVPair<T>> writeBuffer = (ArrayList<KVPair<T>>) new ArrayList(); 
      while (!mergeHead.isEmpty()) {
        MergeData<T> head = mergeHead.poll();
        writeBuffer.add(head.underlying);
        KVPair<T> entry = fileStream.get(head.index).next();
        if (entry != null) {
          mergeHead.add(new MergeData(entry, head.index));
        }
        if (writeBuffer.size() > batchSize) {
          // Flush content of buffer to disk.
          // TODO(nbhagat): Do in a separate thread.
          for (KVPair<T> obj : writeBuffer) {
            oos.writeObject(obj);
          }
          writeBuffer = (ArrayList<KVPair<T>>) new ArrayList();
        }
      }

      if (!writeBuffer.isEmpty()) {
        for (KVPair<T> obj : writeBuffer) {
          oos.writeObject(obj);
        }
      }
      oos.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to merge chunks. Cause: " + ioe.getMessage());
    }
  }

  /**
   * Structure to hold index and data from stream to be merged.
   */
  private static class MergeData<T extends Serializable> implements Comparable<MergeData<T>>, Serializable {
    public int index;
    public String key;
    public T value;
    public KVPair<T> underlying;

    public MergeData(KVPair<T> entry, int index) {
      this.index = index;
      key = entry.key;
      value = entry.value;
      this.underlying = entry;
    }
    @Override
    /**
     * Comparator returns strict ordering wrt key. For value field, order is not
     * preserved. However, this will return 0, iff key, value and index are all equal.
     * Both key must be non-null. Value may or mayn't be null.
     */
    public int compareTo(MergeData<T> obj) {
      if (obj == null) {
        return 1;
      }
      return key.compareTo(obj.key);
    }

    @Override
    public boolean equals(Object other) {
      MergeData<T> obj = (MergeData<T>) other;
      return key.equals(obj.key);
    }
  }
}