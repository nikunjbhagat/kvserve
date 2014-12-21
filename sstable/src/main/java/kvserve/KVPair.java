package kvserve;

import java.io.Serializable;
import java.util.Map;

public class KVPair<T extends Serializable> implements Map.Entry<String, T>, Serializable {
  public final String key;
  public T value;

  public KVPair(String key, T value) {
    this.key = key;
    this.value = value;
  }
  @Override
  public String getKey() {
    return key;
  }
  @Override
  public T getValue() {
    return value;
  }
  @Override
  public T setValue(T value) {
    this.value = value;
    return getValue();
  } 
}