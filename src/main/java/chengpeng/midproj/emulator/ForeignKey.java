package chengpeng.midproj.emulator;

public class ForeignKey {

    private final String attributeName;
    private final String referencedName;

    public ForeignKey(String attributeName, String referencedName) {
        this.attributeName = attributeName;
        this.referencedName = referencedName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getReferencedName() {
        return referencedName;
    }
}
