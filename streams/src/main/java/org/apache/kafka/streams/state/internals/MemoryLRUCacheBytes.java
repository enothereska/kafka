/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory LRU cache store based on HashSet and HashMap. XXX: just stub, does not work
 *
 *  * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.

 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCacheBytes<K, V> implements KeyValueStore<K, V> {
    private final int DEFAULT_CAPACITY = 100;
    private final float DEFAULT_LOAD = 0.75f;
    public interface EldestEntryRemovalListener<K, V> {

        void apply(K key, V value);
    }

    protected Map<K, V> map;
    private volatile boolean open = true;

    protected EldestEntryRemovalListener<K, V> listener;


    public MemoryLRUCacheBytes(final long maxCacheSizeBytes) {
        this.map = new LinkedHashMap<K, V>(DEFAULT_CAPACITY, DEFAULT_LOAD, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return false;
            }
        };
    }



    public MemoryLRUCacheBytes<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
        this.listener = listener;

        return this;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized V get(K key) {
        return this.map.get(key);
    }

    @Override
    public synchronized void put(K key, V value) {
        this.map.put(key, value);
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    @Override
    public synchronized V delete(K key) {
        V value = this.map.remove(key);
        return value;
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    @Override
    public long approximateNumEntries() {
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
    }

    public int size() {
        return this.map.size();
    }
}
