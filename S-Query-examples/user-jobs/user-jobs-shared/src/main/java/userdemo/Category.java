package userdemo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public enum Category implements Serializable {
    CLOTHES("clothes"),
    FOOD("food"),
    TOOLS("tools");

    private final String label;
    private static final Map<String, Category> BY_LABEL = new HashMap<>();

    static {
        for (Category e: values()) {
            BY_LABEL.put(e.label, e);
        }
    }

    Category(String label) {
        this.label = label;
    }

    public final String getLabel() {
        return label;
    }

    public static Category valueOfLabel(String label) {
        return BY_LABEL.get(label);
    }

    @Override
    public String toString() {
        return this.label;
    }
}
