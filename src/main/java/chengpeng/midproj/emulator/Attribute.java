package chengpeng.midproj.emulator;

public class Attribute {
    private final String name;
    private final Class<?> type;

    public Attribute(String name, Class<?> type) {
        this.name = name;
        if (String.class.equals(type)) {
            this.type = String.class;
        } else if (Integer.class.equals(type)) {
            this.type = Integer.class;
        } else {
            throw new IllegalArgumentException("The only allowed data types are Integer and String.");
        }
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return this.type;
    }

    @Override
    public String toString() {
        return "Attribute{" + "name='" + name + '\'' + ", type=" + type.getName() + '}';
    }
}