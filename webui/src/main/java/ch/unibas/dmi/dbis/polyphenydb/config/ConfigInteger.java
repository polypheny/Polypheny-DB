package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigInteger extends Config<Integer> {

    private Integer value;

    public ConfigInteger( String key ) {
        super( key );
        super.setConfigType( "Integer" );
    }

    public ConfigInteger( String key, String description ) {
        super( key, description );
    }

    public Integer getValue() {
        return this.value;
    }

    public void setValue( Integer v ) {
        this.value = v;
    }

    public ConfigInteger withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public String toString() {
        return super.toString();
    }

}
