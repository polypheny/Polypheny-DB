package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigString extends Config<String> {

    private String value;

    public ConfigString ( String key ) {
        super( key );
        super.setConfigType( "String" );
    }

    public ConfigString ( String key, String description ) {
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

    public String toString() {
        return super.toString();
    }

}
