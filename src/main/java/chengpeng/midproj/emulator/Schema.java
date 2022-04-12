package chengpeng.midproj.emulator;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Schema {
    private static final Map<String, Schema> schemas = new LinkedHashMap<>();

    public static Schema getInstance(String name) {
        Schema schema = null;
        if (schemas.containsKey(name)) {
            schemas.get(name);
        } else {
            schemas.put(name, schema = new Schema(name));
        }
        return schema;
    }

    public String getName() {
        return name;
    }

    private final String name;
    private final Map<String, Relation> relations = new TreeMap<>();
    private final Map<String, Set<ForeignKey>> constraints = new TreeMap<>();

    public Schema(String name) {
        this.name = name;
    }

    public Relation create(String name, Collection<Attribute> attributes, Attribute primaryKey) {
        if (relations.containsKey(name)) return relations.get(name);
        Relation relation = new Relation(this, name, attributes, primaryKey);
        relations.put(name, relation);
        return relation;
    }

    public Relation getRelation(String name) {
        return relations.get(name);
    }

    public Set<ForeignKey> getForeignKey(String relationName) {
        if (constraints.containsKey(relationName)) {
            return constraints.get(relationName);
        }
        return null;
    }

    public void addForeignKey(String relationName, String attributeName, String referencedName) {
        if (!relations.containsKey(relationName)) {
            throw new IllegalArgumentException("No such relation");
        }
        if (!relations.containsKey(referencedName)) {
            throw new IllegalArgumentException("No such relation");
        }
        Relation relation = relations.get(relationName);
        Relation referencedRelation = relations.get(referencedName);
        if (referencedRelation.getPrimaryKey() == null) {
            throw new IllegalArgumentException("Foreign key must be referenced relation primary key");
        }
        relation.setHasForeignKey(true);
        referencedRelation.setHasForeignReferenced(true);
        Set<ForeignKey> foreignKeys;
        if (constraints.containsKey(relationName)) {
            foreignKeys = constraints.get(relationName);
        } else {
            constraints.put(relationName, foreignKeys = new HashSet<>());
        }
        foreignKeys.add(new ForeignKey(attributeName, referencedName));
    }

    public boolean checkIsExist(String relationName, String attributeName, Object value) {
        List<ForeignKey> collect = this.constraints.get(relationName).stream().filter(item -> item.getAttributeName().equals(attributeName)).collect(Collectors.toList());
        if (collect.size() != 1) throw new NullPointerException("That not possible");
        ForeignKey foreignKey = collect.get(0);
        Relation relation = relations.get(foreignKey.getReferencedName());
        return relation.checkExist(attributeName, value);
    }

    public boolean checkHasReferenced(String relationName, Object value) {
        for (Map.Entry<String, Set<ForeignKey>> entry : constraints.entrySet()) {
            Relation relation = null;
            for (ForeignKey foreignKey : entry.getValue()) {
                if (!foreignKey.getReferencedName().equals(relationName)) continue;
                if (relation == null) relation = relations.get(entry.getKey());
                if (relation.checkExist(foreignKey.getAttributeName(), value)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isForeignKey(String relationName, String attributeName) {
        return constraints.containsKey(relationName) && constraints.get(relationName).stream().anyMatch(item -> item.getAttributeName().equals(attributeName));
    }

    public void cascadeUpdate(String relationName, String targetName, Object newValue, String selectionName, String... expressions) {
        Map<String, Set<String>> relatedColumns = searchForeign(relationName, targetName);
        for (Map.Entry<String, Set<String>> entry : relatedColumns.entrySet()) {
            var k = entry.getKey();
            var v = entry.getValue();
            Relation relation = relations.get(k);
            for (String columnName : v) {
                if (!columnName.equals(relation.getPrimaryKey().getName())) continue;
                if (relation.checkExist(columnName, newValue)) {
                    throw new IllegalArgumentException("New value will cause foreign key " + k + "." + columnName + " violation");
                }
            }
        }
        if (selectionName.equals(targetName)) {
            for (Map.Entry<String, Set<String>> entry : relatedColumns.entrySet()) {
                var k = entry.getKey();
                var v = entry.getValue();
                Relation relation = relations.get(k);
                for (String columnName : v) {
                    relation.modify(columnName, newValue, columnName, expressions);
                }
            }
        } else {
            Relation requiredRelation = relations.get(relationName);
            List<Object> equalsValue = new LinkedList<>();
            List<Tuple> rows = requiredRelation.getRows();
            Class<?> clazz = requiredRelation.getDefine().get(selectionName);
            if (clazz == String.class) {
                Set<Object> collect = Arrays.stream(expressions).collect(Collectors.toUnmodifiableSet());
                for (Tuple row : rows) {
                    if (!collect.contains(row.getValue(selectionName))) continue;
                    equalsValue.add(row.getValue(targetName));
                }
            } else {
                if (expressions.length != 1) throw new IllegalArgumentException("Integer allow only one expression such as =5, <100 and >30");
                String expression = expressions[0];
                int number = Integer.parseInt(expression.substring(1));
                switch (expression.charAt(0)) {
                    case '=': {
                        for (Tuple row : rows) {
                            if (row.getValue(selectionName).equals(number)) {
                                equalsValue.add(row.getValue(targetName));
                            }
                        }
                        break;
                    }
                    case '<': {
                        for (Tuple row : rows) {
                            if ((Integer) row.getValue(selectionName) < number) {
                                equalsValue.add(row.getValue(targetName));
                            }
                            break;
                        }
                    }
                    case '>': {
                        for (Tuple row : rows) {
                            if ((Integer) row.getValue(selectionName) > number) {
                                equalsValue.add(row.getValue(targetName));
                            }
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Invalidate expression");
                }
            }
            for (Map.Entry<String, Set<String>> entry : relatedColumns.entrySet()) {
                var k = entry.getKey();
                var v = entry.getValue();
                Relation relation = relations.get(k);
                for (String columnName : v) {
                    if (k.equals(relationName) && columnName.equals(targetName)) continue;
                    relation.modifyByEquals(columnName, newValue, equalsValue);
                }
            }
        }
    }

    public Map<String, Set<String>> searchForeign(String relationName, String columnName) {
        List<ForeignKeySearch> searchList = new LinkedList<>();
        searchList.add(new ForeignKeySearch(relationName, columnName, null, null));
        searchForeign(searchList);
        searchList.remove(0);
        Map<String, Set<String>> relationColumns = new HashMap<>();
        for (ForeignKeySearch foreignKeySearch : searchList) {
            Set<String> columns;
            {
                String tableName = foreignKeySearch.relationName;
                if (relationColumns.containsKey(tableName)) {
                    columns = relationColumns.get(tableName);
                } else {
                    relationColumns.put(tableName, columns = new HashSet<>());
                }
                if (tableName.equals(relationName) && foreignKeySearch.columnName.equals(columnName)) continue;
                columns.add(foreignKeySearch.columnName);
            }
            {
                String tableName = foreignKeySearch.referName;
                if (relationColumns.containsKey(tableName)) {
                    columns = relationColumns.get(tableName);
                } else {
                    relationColumns.put(tableName, columns = new HashSet<>());
                }
                if (tableName.equals(relationName) && foreignKeySearch.columnName.equals(columnName)) continue;
                columns.add(foreignKeySearch.referColumn);
            }
        }
        return relationColumns;
    }

    public void searchForeign(List<ForeignKeySearch> searchList) {
        ForeignKeySearch lastSearch = searchList.get(searchList.size() - 1);
        String relationName;
        String columnName;
        if (lastSearch.referName == null && lastSearch.referColumn == null) {
            relationName = lastSearch.relationName;
            columnName = lastSearch.columnName;
        } else {
            relationName = lastSearch.referName;
            columnName = lastSearch.referColumn;
        }
        Relation relation = relations.get(relationName);
        loop:
        for (Map.Entry<String, Set<ForeignKey>> entry : constraints.entrySet()) {
            for (ForeignKey foreignKey : entry.getValue()) {
                //noinspection ConstantConditions
                do {
                    // 被作为外键
                    if (foreignKey.getReferencedName().equals(relationName)) {
                        String referColumn = relation.getPrimaryKey().getName();
                        ForeignKeySearch foreignKeySearch = new ForeignKeySearch(entry.getKey(), foreignKey.getAttributeName(), relationName, referColumn);
                        if (searchList.contains(foreignKeySearch)) break;
                        searchList.add(foreignKeySearch);
                        searchForeign(searchList);
                    }
                } while (false);
                // 包含外键引用
                if (foreignKey.getAttributeName().equals(columnName)) {
                    String referencedName = foreignKey.getReferencedName();
                    String referColumn = relations.get(referencedName).getPrimaryKey().getName();
                    ForeignKeySearch foreignKeySearch = new ForeignKeySearch(relationName, columnName, referencedName, referColumn);
                    if (searchList.contains(foreignKeySearch)) break loop;
                    searchList.add(foreignKeySearch);
                    searchForeign(searchList);
                }
            }
        }
    }

    public static class ForeignKeySearch {
        private final String relationName;
        private final String columnName;
        private final String referName;
        private final String referColumn;

        public ForeignKeySearch(String relationName, String columnName, String referName, String referColumn) {
            this.relationName = relationName;
            this.columnName = columnName;
            this.referName = referName;
            this.referColumn = referColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ForeignKeySearch)) return false;
            ForeignKeySearch that = (ForeignKeySearch) o;
            return
                this.relationName.equals(that.relationName) &&
                    this.columnName.equals(that.columnName) &&
                    this.referName.equals(that.referName) &&
                    this.referColumn.equals(that.referColumn);
        }

        @Override
        public String toString() {
            return "{" + relationName + '.' + columnName + " -> " + referName + '.' + referColumn + '}';
        }
    }
}