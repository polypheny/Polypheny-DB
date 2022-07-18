package org.polypheny.db.languages.polyscript;

public class SqlExpression extends Expression{

    public SqlExpression(String value) {
        super(value);
    }

    @Override
    Expression newInstance(String script) {
        return new SqlExpression(script);
    }
}
