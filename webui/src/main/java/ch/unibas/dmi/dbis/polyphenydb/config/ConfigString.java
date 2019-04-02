package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigString<String> extends Config<String> {

    private String value;

    public ConfigString ( java.lang.String key ) {
        super( key );
        super.setConfigType( "String" );
    }

    public ConfigString ( java.lang.String key, java.lang.String description ) {
        super( key, description );
    }

    public String getValue() {
        return this.value;
    }

    public void setValue( String v ) {
        this.value = v;
    }

    public ConfigString withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public java.lang.String toString() {
        return super.toString();
    }

}
