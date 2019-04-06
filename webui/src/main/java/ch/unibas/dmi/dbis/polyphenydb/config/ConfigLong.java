package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigLong extends ConfigNumber {

    long value;

    public ConfigLong ( String key ) {
        super( key );
    }

    public ConfigLong ( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object o ) {
        this.value = ( long ) o;
        notifyConfigListeners( this );
    }

    @Override
    public Long getLong() {
        return this.value;
    }

    @Override
    public void setLong( long value ) {
        this.value = value;
        notifyConfigListeners( this );
    }
}
