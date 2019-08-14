package io.github.belugabehr.datanode.util;

import java.util.ArrayList;
import java.util.Collection;

public class Batch<K, V> {

  private final K key;
  private final Collection<V> items;

  public Batch(final K key) {
    this.key = key;
    this.items = new ArrayList<>();
  }

  public K getKey() {
    return this.key;
  }

  public Collection<V> getItems() {
    return this.items;
  }

  public boolean add(V item) {
    return this.items.add(item);
  }

  @Override
  public String toString() {
    return "Batch [key=" + this.key + ", items=" + this.items + "]";
  }

}
