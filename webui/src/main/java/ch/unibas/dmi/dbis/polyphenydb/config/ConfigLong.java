package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigLong extends ConfigNumber {

    long value;

    public ConfigLong ( String key, long value ) {
        this.key = key;
        this.value = value;
    }

    public ConfigLong ( String key, String description, long value ) {
        this.key = key;
        this.description = description;
        this.value = value;
    }

    @Override
    public void setObject( Object o ) {
        this.value = ( long ) o;
        notifyConfigListeners( this );
    }

    @Override
    public long getLong() {
        return this.value;
    }

    @Override
    public void setLong( long value ) {
        this.value = value;
        notifyConfigListeners( this );
    }
}
