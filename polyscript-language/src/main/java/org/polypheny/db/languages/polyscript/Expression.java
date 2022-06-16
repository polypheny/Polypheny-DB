package org.polypheny.db.languages.polyscript;

public class Expression {
    private final String value;


    public Expression(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return getClass().getName() + ": { expression: " + value + "}";
    }
}
