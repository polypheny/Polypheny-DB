package ch.unibas.dmi.dbis.polyphenydb.config;


/** type of the config for the WebUi to specify how it should be rendered in the UI (&lt;input type="text/number/etc."&gt;)
 * e.g. text or number */
public enum WebUiFormType{
    TEXT("text"),
    NUMBER("number");

    private final String type;

    WebUiFormType( String t ) {
        this.type = t;
    }

    @Override
    public String toString() {
        return this.type;
    }
}