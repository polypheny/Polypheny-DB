package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigNumber<Number> extends Config<Number> {

    private Number value;

    public ConfigNumber ( String key ) {
        super( key );
        super.setConfigType( "Number" );
    }

    public ConfigNumber ( String key, String description ) {
        super( key, description );
    }

    public Number getValue() {
        return this.value;
    }

    public void setValue( Number v ) {
        this.value = v;
    }

    public ConfigNumber withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public String toString() {
        return super.toString();
    }

}
