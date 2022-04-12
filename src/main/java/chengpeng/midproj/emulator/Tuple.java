package chengpeng.midproj.emulator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Tuple {
    private final Map<String, Object> values;

    public Tuple(Map<String, Object> values) {
        Set<Integer> temp = new HashSet<>();
        for (Object value : values.values()) {
            if (value == null) continue;
            if (value instanceof Integer) continue;
            if (value instanceof String) continue;
            int hash = value.hashCode();
            if (temp.contains(hash)) {
                throw new IllegalArgumentException("Value has duplicate hash " + value + "-" + hash);
            } else {
                temp.add(hash);
            }
        }
        this.values = new HashMap<>(values);
    }

    public int size() {
        return values.size();
    }

    public Tuple addPrefix(String prefix) {
        Map<String, Object> temp = new HashMap<>(this.values.size());
        this.values.forEach((k, v) -> temp.put(prefix + "_" + k, v));
        return new Tuple(temp);
    }

    public static Tuple merge(Tuple... tuples) {
        Map<String, Object> temp = new HashMap<>();
        for (Tuple tuple : tuples) {
            tuple.values.forEach(temp::put);
        }
        return new Tuple(temp);
    }

    public boolean containsKey(String name) {
        return values.containsKey(name);
    }

    public Object getValue(String attributeName) {
        return values.get(attributeName);
    }

    public Set<String> getKeys() {
        return values.keySet();
    }

    public Collection<Object> getValues() {
        return values.values();
    }

    public void set(String name, Object value) {
        this.values.put(name, value);
    }

    public Map<String, Object> getDataStore() {
        return values;
    }

    public boolean isValueSame(Tuple tuple) {
        if (this.size() != tuple.size()) return false;
        Map<String, Object> other = tuple.getDataStore();
        for (Map.Entry<String, Object> entry : this.values.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            if (!other.containsKey(k)) return false;
            if (!other.get(k).equals(v)) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Tuple{");
        for (Map.Entry<String, Object> entry : this.values.entrySet()) {
            builder.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        }
        builder.append("}");
        return builder.toString();
    }

    public static TupleBuilder builder() {
        return new TupleBuilder();
    }

    public static class TupleBuilder {
        private final Map<String, Object> data = new HashMap<>();

        private TupleBuilder() { }

        public TupleBuilder add(String name, Object object) {
            data.put(name, object);
            return this;
        }

        public Tuple build() {
            return new Tuple(this.data);
        }

        public int size() {
            return this.data.size();
        }
    }
}