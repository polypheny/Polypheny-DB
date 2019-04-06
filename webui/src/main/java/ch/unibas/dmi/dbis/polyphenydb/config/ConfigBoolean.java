package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigBoolean extends ConfigScalar {

    boolean value;

    public ConfigBoolean ( String key ) {
        super( key );
    }

    public ConfigBoolean ( String key, String description ) {
        super( key, description );
    }

    @Override
    public Boolean getBoolean() {
        return this.value;
    }

    @Override
    public void setBoolean( boolean b ) {
        this.value = b;
        notifyConfigListeners( this );
    }

    @Override
    public void setObject( Object o ) {
        this.value = ( boolean ) o;
        notifyConfigListeners( this );
    }

}
