package chengpeng.midproj.emulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class Relation {
    private final Schema schema;
    private final String name;
    private Attribute key;

    public Attribute getPrimaryKey() {
        return key;
    }

    private List<Attribute> cols;

    public boolean isHasForeignKey() {
        return hasForeignKey;
    }

    public void setHasForeignKey(boolean hasForeignKey) {
        this.hasForeignKey = hasForeignKey;
    }

    public boolean isHasForeignReferenced() {
        return hasForeignReferenced;
    }

    public void setHasForeignReferenced(boolean hasForeignReferenced) {
        this.hasForeignReferenced = hasForeignReferenced;
    }

    private boolean hasForeignKey;
    private boolean hasForeignReferenced;

    public int getColumnSize() {
        return columnSize;
    }

    private int columnSize;
    private List<Tuple> rows;

    public Relation(Schema schema, String name) {
        this.schema = schema;
        this.name = name;
        this.rows = new CopyOnWriteArrayList<>();
    }

    public Relation(Schema schema, String name, Collection<Attribute> attributes) {
        this(schema, name);
        this.cols = new ArrayList<>(attributes);
        this.columnSize = attributes.size();
    }

    public Relation(Schema schema, String name, Collection<Attribute> attributes, Collection<Tuple> tuples) {
        this(schema, name, attributes);
        this.insert(tuples);
    }

    public Relation(Schema schema, String name, Collection<Attribute> attributes, Attribute key) {
        this(schema, name, attributes);
        if (!this.getDefine().containsKey(key.getName())) {
            throw new IllegalArgumentException("Primary Key not exist in columns define");
        }
        this.key = key;
    }

    public Relation(Schema schema, String name, Collection<Attribute> attributes, Attribute key, Collection<Tuple> tuples) {
        this(schema, name, attributes, key);
        this.insert(tuples);
    }

    public String getName() {
        return name;
    }

    public Map<String, Class<?>> getDefine() {
        Map<String, Class<?>> define = new HashMap<>(this.columnSize);
        this.cols.forEach(item -> define.put(item.getName(), item.getType()));
        return define;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isSameDefine(Relation that) {
        if (this.columnSize != that.getColumnSize()) return false;
        Map<String, Class<?>> thatDefine = that.getDefine();
        for (Map.Entry<String, Class<?>> entry : this.getDefine().entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            if (!thatDefine.containsKey(k)) return false;
            if (!thatDefine.get(k).equals(v)) return false;
        }
        return true;
    }

    public void insert(Tuple tuple) {
        if (tuple == null) {
            throw new IllegalArgumentException("Tuples collection contains null");
        }
        if (rows.contains(tuple)) {
            throw new IllegalArgumentException("Tuple duplicated " + tuple);
        }
        if (columnSize != tuple.size()) {
            throw new IllegalArgumentException("Tuple size mismatch with define " + tuple);
        }
        for (String attributeName : tuple.getKeys()) {
            if (!this.getDefine().containsKey(attributeName)) {
                throw new IllegalArgumentException("Tuple contains column not define " + tuple);
            }
        }
        for (Attribute col : cols) {
            Object value = tuple.getValue(col.getName());
            if (value instanceof NullValue) continue;
            if (!col.getType().isInstance(value)) {
                throw new IllegalArgumentException("Column domain check failed " + col + ">" + value);
            }
        }
        if (key != null) {
            if (tuple.getValue(this.key.getName()) == null) {
                throw new IllegalArgumentException("Tuples has no value for primaryKey " + tuple);
            }
            if (rows.stream().anyMatch(item -> item.getValue(this.key.getName()).equals(tuple.getValue(this.key.getName())))) {
                throw new IllegalArgumentException("PrimaryKey has duplication value " + tuple);
            }
        }
        if (hasForeignKey) {
            Set<ForeignKey> foreignKeys = schema.getForeignKey(this.name);
            for (ForeignKey foreignKey : foreignKeys) {
                String attributeName = foreignKey.getAttributeName();
                if (!schema.checkIsExist(this.name, attributeName, tuple.getValue(attributeName))) {
                    throw new IllegalArgumentException("Foreign key not exist " + tuple);
                }
            }
        }
        rows.add(tuple);
    }

    public void insert(Collection<Tuple> tuples) {
        tuples.forEach(this::insert);
    }

    public Relation delete(String name, String expression) {
        Map<String, Class<?>> define = this.getDefine();
        if (!define.containsKey(name)) {
            throw new IllegalArgumentException("No such column");
        }
        Class<?> clazz = define.get(name);
        if (clazz == String.class) {
            List<Tuple> list = new ArrayList<>();
            for (Tuple row : rows) {
                if (row.getValue(name).equals(expression)) {
                    if (hasForeignReferenced) {
                        if (schema.checkHasReferenced(this.name, row.getValue(this.key.getName()))) {
                            throw new IllegalArgumentException("Can't delete there is foreign key reference for " + row);
                        }
                    }
                    continue;
                }
                list.add(row);
            }
            rows = list;
        } else {
            int number = Integer.parseInt(expression.substring(1));
            switch (expression.charAt(0)) {
                case '=': {
                    List<Tuple> list = new ArrayList<>();
                    for (Tuple row : rows) {
                        if (row.getValue(name).equals(number)) {
                            if (hasForeignReferenced) {
                                if (schema.checkHasReferenced(this.name, row.getValue(this.key.getName()))) {
                                    throw new IllegalArgumentException("There is foreign reference for " + row);
                                }
                            }
                        }
                        list.add(row);
                    }
                    rows = list;
                    break;
                }
                case '<': {
                    List<Tuple> list = new ArrayList<>();
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(name) > number - 1) {
                            if (hasForeignReferenced) {
                                if (schema.checkHasReferenced(this.name, row.getValue(this.key.getName()))) {
                                    throw new IllegalArgumentException("There is foreign reference for " + row);
                                }
                            }
                        }
                        list.add(row);
                    }
                    rows = list;
                    break;
                }
                case '>': {
                    List<Tuple> list = new ArrayList<>();
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(name) < number + 1) {
                            if (hasForeignReferenced) {
                                if (schema.checkHasReferenced(this.name, row.getValue(this.key.getName()))) {
                                    throw new IllegalArgumentException("There is foreign reference for " + row);
                                }
                            }
                        }
                        list.add(row);
                    }
                    rows = list;
                    break;
                }
                default:
                    throw new IllegalArgumentException("Invalidate expression");
            }
        }
        return this;
    }

    public void update(String targetName, Object newValue, String selectionName, String... expressions) {
        Map<String, Class<?>> define = this.getDefine();
        if (!define.containsKey(selectionName)) {
            throw new IllegalArgumentException("No such column " + selectionName);
        }
        if (!define.containsKey(targetName)) {
            throw new IllegalArgumentException("No such column " + targetName);
        }
        if (this.key.getName().equals(targetName)) {
            if (this.getAllValue(this.key.getName()).contains(newValue)) {
                throw new IllegalArgumentException("Update will cause duplicate primary key " + newValue);
            }
        }
        if (this.hasForeignReferenced || (hasForeignKey && schema.isForeignKey(this.name, targetName))) {
            this.schema.cascadeUpdate(this.name, targetName, newValue, selectionName, expressions);
        }
        if (!targetName.equals(selectionName)) {
            this.modify(targetName, newValue, selectionName, expressions);
        }
    }

    protected void modify(String targetName, Object newValue, String selectionName, String... expressions) {
        Class<?> clazz = this.getDefine().get(selectionName);
        if (clazz == String.class) {
            Set<Object> collect = Arrays.stream(expressions).collect(Collectors.toUnmodifiableSet());
            for (Tuple row : rows) {
                if (!collect.contains(row.getValue(selectionName))) continue;
                row.set(targetName, newValue);
            }
        } else {
            if (expressions.length != 1) throw new IllegalArgumentException("Integer allow only one expression such as =5, <100 and >30");
            String expression = expressions[0];
            int number = Integer.parseInt(expression.substring(1));
            switch (expression.charAt(0)) {
                case '=': {
                    for (Tuple row : rows) {
                        if (row.getValue(selectionName).equals(number)) {
                            row.set(targetName, newValue);
                        }
                    }
                    break;
                }
                case '<': {
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(selectionName) < number) {
                            row.set(targetName, newValue);
                        }
                    }
                    break;
                }
                case '>': {
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(selectionName) > number) {
                            row.set(targetName, newValue);
                        }
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Invalidate expression");
            }
        }
    }

    public void modifyByEquals(String columnName, Object newValue, List<Object> equalsValue) {
        for (Tuple row : rows) {
            if (!equalsValue.contains(row.getValue(columnName))) continue;
            row.set(columnName, newValue);
        }
    }

    public Relation projection(String... names) {
        Map<String, Class<?>> define = this.getDefine();
        List<Attribute> newDefine = new ArrayList<>(names.length);
        for (String name : names) {
            if (define.containsKey(name)) {
                newDefine.add(new Attribute(name, define.get(name)));
            } else {
                throw new IllegalArgumentException("No such column");
            }
        }
        Relation relation = new Relation(null, "result", newDefine);
        Map<String, Object> temp = new HashMap<>(rows.size());
        for (Tuple row : rows) {
            for (String name : names) {
                temp.put(name, row.getValue(name));
            }
            relation.insert(new Tuple(temp));
        }
        return relation;
    }

    public Relation selection(String name, String... expressions) {
        Map<String, Class<?>> define = this.getDefine();
        if (!define.containsKey(name)) {
            throw new IllegalArgumentException("No such column");
        }
        Relation relation = new Relation(null, "result", this.cols);
        Class<?> clazz = define.get(name);
        if (clazz == String.class) {
            Set<Object> collect = Arrays.stream(expressions).collect(Collectors.toUnmodifiableSet());
            for (Tuple row : rows) {
                if (collect.contains(row.getValue(name))) {
                    relation.insert(row);
                }
            }
        } else {
            if (expressions.length != 1) throw new IllegalArgumentException("");
            String expression = expressions[0];
            Integer number = Integer.parseInt(expression.substring(1));
            switch (expression.charAt(0)) {
                case '=': {
                    for (Tuple row : rows) {
                        if (row.getValue(name).equals(number)) {
                            relation.insert(row);
                        }
                    }
                    break;
                }
                case '<': {
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(name) < number) {
                            relation.insert(row);
                        }
                    }
                    break;
                }
                case '>': {
                    for (Tuple row : rows) {
                        if ((Integer) row.getValue(name) > number) {
                            relation.insert(row);
                        }
                    }
                    break;
                }
                default:
            }
        }
        return relation;
    }

    public Relation union(Relation that) {
        if (!isSameDefine(that)) throw new IllegalArgumentException("Not same structure");
        List<Tuple> result = new LinkedList<>(this.rows);
        for (Tuple row : that.rows) {
            if (result.stream().noneMatch(row::isValueSame)) result.add(row);
        }
        return new Relation(null, "result", this.cols, result);
    }

    public Relation intersection(Relation that) {
        if (!isSameDefine(that)) throw new IllegalArgumentException("Not same structure");
        List<Tuple> result = new LinkedList<>();
        for (Tuple row : this.rows) {
            for (Tuple tuple : that.rows) {
                if (row.isValueSame(tuple)) result.add(row);
            }
        }
        return new Relation(null, "result", this.cols, result);
    }

    public Relation difference1(Relation that) {
        if (!isSameDefine(that)) throw new IllegalArgumentException("Not same structure");
        List<Tuple> result = new LinkedList<>();
        loop:
        for (Tuple row : this.rows) {
            for (Tuple tuple : that.rows) {
                if (row.isValueSame(tuple)) continue loop;
            }
            result.add(row);
        }
        loop:
        for (Tuple row : that.rows) {
            for (Tuple tuple : this.rows) {
                if (row.isValueSame(tuple)) continue loop;
            }
            result.add(row);
        }
        return new Relation(null, "result", this.cols, result);
    }

    public Relation difference2(Relation that) {
        if (!isSameDefine(that)) throw new IllegalArgumentException("Not same structure");
        List<Tuple> result = new LinkedList<>();
        Relation union = this.union(that);
        Relation intersection = this.intersection(that);
        loop:
        for (Tuple row : union.rows) {
            for (Tuple tuple : intersection.rows) {
                if (row.isValueSame(tuple)) continue loop;
            }
            result.add(row);
        }
        return new Relation(null, "result", this.cols, result);
    }

    public Relation cross(Relation that) {
        if (this.name.equals(that.name)) throw new IllegalArgumentException("Can't join same name relation");
        Map<String, Class<?>> thisDefine = this.getDefine();
        Map<String, Class<?>> thatDefine = that.getDefine();
        List<Attribute> attributes = new ArrayList<>(thisDefine.size() + thatDefine.size());
        for (Map.Entry<String, Class<?>> entry : thisDefine.entrySet()) {
            attributes.add(new Attribute(this.name + "_" + entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Class<?>> entry : thatDefine.entrySet()) {
            attributes.add(new Attribute(that.name + "_" + entry.getKey(), entry.getValue()));
        }
        Relation relation = new Relation(null, "result", attributes);
        for (Tuple thisRow : this.rows) {
            for (Tuple thatRow : that.rows) {
                Tuple thisNewTuple = thisRow.addPrefix(this.name);
                Tuple thatNewTuple = thatRow.addPrefix(that.name);
                Tuple merge = Tuple.merge(thisNewTuple, thatNewTuple);
                relation.insertFillNull(merge);
            }
        }
        return relation;
    }

    public Relation equiJoin(Relation that, String name) {
        Map<String, Class<?>> thisDefine = this.getDefine();
        Map<String, Class<?>> thatDefine = that.getDefine();
        if (!thisDefine.containsKey(name) || !thatDefine.containsKey(name) || thisDefine.get(name) != thatDefine.get(name)) {
            throw new IllegalArgumentException("EqualJoin need two relation contains column has same name and type");
        }
        Relation relation = makeJoinRelationNoRepeat(thisDefine, thatDefine);
        loop:
        for (Tuple thisRow : this.rows) {
            for (Tuple thatRow : that.rows) {
                if (thisRow.getValue(name).equals(thatRow.getValue(name))) {
                    relation.insert(Tuple.merge(thisRow, thatRow));
                    continue loop;
                }
            }
        }
        return relation;
    }

    public Relation leftJoin(Relation that, String name) {
        Map<String, Class<?>> thisDefine = this.getDefine();
        Map<String, Class<?>> thatDefine = that.getDefine();
        if (!thisDefine.containsKey(name) || !thatDefine.containsKey(name) || thisDefine.get(name) != thatDefine.get(name)) {
            throw new IllegalArgumentException("LeftJoin need two relation contains column has same name and type");
        }
        Relation relation = makeJoinRelationNoRepeat(thisDefine, thatDefine);
        loop:
        for (Tuple thisRow : this.rows) {
            for (Tuple thatRow : that.rows) {
                if (thisRow.getValue(name).equals(thatRow.getValue(name))) {
                    relation.insert(Tuple.merge(thisRow, thatRow));
                    continue loop;
                }
            }
            relation.insertFillNull(thisRow);
        }
        return relation;
    }

    private Relation makeJoinRelationNoRepeat(Map<String, Class<?>> thisDefine, Map<String, Class<?>> thatDefine) {
        List<Attribute> attributes = new ArrayList<>(thisDefine.size() + thatDefine.size());
        Set<String> temp = new HashSet<>();
        for (Map.Entry<String, Class<?>> entry : thisDefine.entrySet()) {
            String key = entry.getKey();
            if (!temp.contains(key)) {
                temp.add(key);
                attributes.add(new Attribute(key, entry.getValue()));
            }
        }
        for (Map.Entry<String, Class<?>> entry : thatDefine.entrySet()) {
            String key = entry.getKey();
            if (!temp.contains(key)) {
                temp.add(key);
                attributes.add(new Attribute(key, entry.getValue()));
            }
        }
        return new Relation(null, "result", attributes);
    }

    private Relation makeJoinRelationNewName(Relation that, Map<String, Class<?>> thisDefine, Map<String, Class<?>> thatDefine) {
        List<Attribute> attributes = new ArrayList<>(thisDefine.size() + thatDefine.size());
        for (Map.Entry<String, Class<?>> entry : thisDefine.entrySet()) {
            attributes.add(new Attribute(this.name + "_" + entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Class<?>> entry : thatDefine.entrySet()) {
            attributes.add(new Attribute(that.name + "_" + entry.getKey(), entry.getValue()));
        }
        return new Relation(null, "result", attributes);
    }

    public Relation naturalJoin(Relation that) {
        Map<String, Class<?>> thisDefine = this.getDefine();
        Map<String, Class<?>> thatDefine = that.getDefine();
        String joinName = null;
        for (String name : thisDefine.keySet()) {
            if (thatDefine.containsKey(name)) {
                if (joinName != null) throw new IllegalArgumentException("Naturel join need one and only one same name column");
                joinName = name;
            }
        }
        if (joinName == null) throw new IllegalArgumentException("Nature join need at least one same name column");
        return equiJoin(that, joinName);
    }

    private Relation makeAggregateResultRelation(String name, int number) {
        List<Attribute> attributes = new ArrayList<>(1);
        attributes.add(new Attribute(name, Integer.class));
        Relation relation = new Relation(null, name, attributes);
        Map<String, Object> temp = new HashMap<>(1);
        temp.put(name, number);
        relation.insert(new Tuple(temp));
        return relation;
    }

    public Relation count(String name) {
        if (!this.getDefine().containsKey(name)) throw new IllegalArgumentException("No such column");
        return makeAggregateResultRelation("count_" + name, rows.size());
    }

    public Relation min(String name) {
        checkIsInteger(name);
        List<Object> values = getAllValue(name);
        values.sort(Comparator.comparingInt(o -> (Integer) o));
        return makeAggregateResultRelation("min_" + name, (Integer) values.get(0));
    }

    public Relation max(String name) {
        checkIsInteger(name);
        List<Object> values = getAllValue(name);
        values.sort((o1, o2) -> (Integer) o2 - (Integer) o1);
        return makeAggregateResultRelation("max_" + name, (Integer) values.get(0));
    }

    public Relation average(String name) {
        checkIsInteger(name);
        List<Object> values = getAllValue(name);
        BigInteger integer = BigInteger.ZERO;
        for (Object value : values) {
            integer = integer.add(BigInteger.valueOf((Integer) value));
        }
        int of = Integer.parseInt(integer.divide(BigInteger.valueOf(values.size())).toString());
        return makeAggregateResultRelation("average_" + name, of);
    }

    public Relation sum(String name) {
        checkIsInteger(name);
        List<Object> values = getAllValue(name);
        BigInteger integer = BigInteger.ZERO;
        for (Object value : values) {
            integer = integer.add(BigInteger.valueOf((Integer) value));
        }
        int of = Integer.parseInt(integer.toString());
        return makeAggregateResultRelation("sum_" + name, of);
    }

    private List<Object> getAllValue(String name) {
        List<Object> result = new ArrayList<>(this.rows.size());
        for (Tuple row : this.rows) {
            result.add(row.getValue(name));
        }
        return result;
    }

    private Relation makeGroupingResult(String name, String type) {
        List<Attribute> attributes = new ArrayList<>(2);
        attributes.add(new Attribute(name, this.getDefine().get(name)));
        attributes.add(new Attribute(type, Integer.class));
        return new Relation(null, name, attributes);
    }

    private Map<Object, List<Tuple>> grouping(String name) {
        Map<Object, List<Tuple>> counting = new HashMap<>();
        for (Tuple row : rows) {
            Object value = row.getValue(name);
            List<Tuple> temp;
            if (counting.containsKey(value)) {
                temp = counting.get(value);
            } else {
                counting.put(value, temp = new LinkedList<>());
            }
            temp.add(row);
        }
        return counting;
    }

    public Relation groupingCount(String name) {
        if (!this.getDefine().containsKey(name)) throw new IllegalArgumentException("No such column");
        Relation relation = makeGroupingResult(name, "group_count_" + name);
        Map<Object, List<Tuple>> grouped = grouping(name);
        for (Map.Entry<Object, List<Tuple>> entry : grouped.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            relation.insert(Tuple.builder()
                                .add(name, k)
                                .add("group_count_" + name, v.size())
                                .build()
            );
        }
        return relation;
    }

    public Relation groupingMin(String groupBy, String name) {
        checkIsExist(groupBy);
        checkIsInteger(name);
        String columnName = "group_by_" + groupBy + "_min_" + name;
        Relation relation = makeGroupingResult(groupBy, columnName);
        Map<Object, List<Tuple>> grouped = grouping(groupBy);
        for (Map.Entry<Object, List<Tuple>> entry : grouped.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            v.sort(Comparator.comparingInt(o -> (int) o.getValue(name)));
            relation.insert(Tuple.builder()
                                .add(groupBy, k)
                                .add(columnName, v.get(0).getValue(name))
                                .build()
            );
        }
        return relation;
    }

    public Relation groupingMax(String groupBy, String name) {
        checkIsExist(groupBy);
        checkIsInteger(name);
        String columnName = "group_by_" + groupBy + "_max_" + name;
        Relation relation = makeGroupingResult(groupBy, columnName);
        Map<Object, List<Tuple>> grouped = grouping(groupBy);
        for (Map.Entry<Object, List<Tuple>> entry : grouped.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            v.sort((o1, o2) -> (int) o2.getValue(name) - (int) o1.getValue(name));
            relation.insert(Tuple.builder()
                                .add(groupBy, k)
                                .add(columnName, v.get(0).getValue(name))
                                .build()
            );
        }
        return relation;
    }

    public Relation groupingSum(String groupBy, String name) {
        checkIsExist(groupBy);
        checkIsInteger(name);
        String columnName = "group_by_" + groupBy + "_sum_" + name;
        Relation relation = makeGroupingResult(groupBy, columnName);
        Map<Object, List<Tuple>> grouped = grouping(groupBy);
        for (Map.Entry<Object, List<Tuple>> entry : grouped.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            BigInteger integer = BigInteger.ZERO;
            for (Tuple tuple : v) integer = integer.add(BigInteger.valueOf((int) tuple.getValue(name)));
            int of = Integer.parseInt(integer.toString());
            relation.insert(Tuple.builder()
                                .add(groupBy, k)
                                .add(columnName, of)
                                .build()
            );
        }
        return relation;
    }

    public Relation groupingAverage(String groupBy, String name) {
        checkIsExist(groupBy);
        checkIsInteger(name);
        String columnName = "group_by_" + groupBy + "_average_" + name;
        Relation relation = makeGroupingResult(groupBy, columnName);
        Map<Object, List<Tuple>> grouped = grouping(groupBy);
        for (Map.Entry<Object, List<Tuple>> entry : grouped.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            BigInteger integer = BigInteger.ZERO;
            for (Tuple tuple : v) integer = integer.add(BigInteger.valueOf((int) tuple.getValue(name)));
            int of = Integer.parseInt(integer.divide(BigInteger.valueOf(v.size())).toString());
            relation.insert(Tuple.builder()
                                .add(groupBy, k)
                                .add(columnName, of)
                                .build()
            );
        }
        return relation;
    }

    public Relation sort(String name, boolean desc) {
        if (!this.getDefine().containsKey(name)) throw new IllegalArgumentException("No such column");
        Relation relation = new Relation(null, "result", this.cols, this.rows);
        if (this.getDefine().get(name).equals(String.class)) {
            if (desc) {
                relation.rows.sort((o1, o2) -> ((String) o2.getValue(name)).compareToIgnoreCase(((String) o1.getValue(name))));
            } else {
                relation.rows.sort((o1, o2) -> ((String) o1.getValue(name)).compareToIgnoreCase(((String) o2.getValue(name))));
            }
        } else {
            if (desc) {
                relation.rows.sort((o1, o2) -> ((int) o2.getValue(name)) - ((int) o1.getValue(name)));
            } else {
                relation.rows.sort(Comparator.comparingInt(o -> ((int) o.getValue(name))));
            }
        }
        return relation;
    }

    protected boolean checkExist(String col, Object value) {
        return rows.stream().anyMatch(item -> item.getValue(col).equals(value));
    }

    private void checkIsExist(String name) {
        if (!this.getDefine().containsKey(name)) throw new IllegalArgumentException("No such column");
    }

    private void checkIsInteger(String name) {
        if (!this.getDefine().containsKey(name)) throw new IllegalArgumentException("No such column");
        if (!this.getDefine().get(name).equals(Integer.class)) throw new IllegalArgumentException("Column not integer");
    }

    private void insertFillNull(Tuple tuple) {
        Tuple.TupleBuilder tupleBuilder = Tuple.builder();
        for (Attribute col : this.cols) {
            String name = col.getName();
            tupleBuilder.add(name, tuple.containsKey(name) ? tuple.getValue(name) : new NullValue());
        }
        this.insert(tupleBuilder.build());
    }

    public String[] getAllColumnsName() {
        return this.cols.stream().map(Attribute::getName).toArray(String[]::new);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.name);
        builder.append("\n");
        Set<String> keySet = this.getDefine().keySet();
        for (String name : keySet) {
            builder.append(String.format("%15s", name)).append("\t");
        }
        builder.append("\n");
        for (Tuple row : rows) {
            for (String name : keySet) {
                builder.append(String.format("%15s", row.getValue(name))).append("\t");
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    public Relation printTable(String title) {
        System.out.println(title + " -> " + toString() + "\n");
        return this;
    }

    public List<Tuple> getRows() {
        return rows;
    }

    public static class NullValue {
        @Override
        public String toString() {
            return "null";
        }
    }
}