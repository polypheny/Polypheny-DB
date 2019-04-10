package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationProgress extends Information {

    private String label;
    private int value;
    private String color = "dynamic";
    private int min = 0;
    private int max = 100;


    public InformationProgress ( String id, String group, String label, int value ) {
        super( id, group );
        this.type = InformationType.PROGRESS;
        this.label = label;
        this.value = value;
    }

    /**@param color default: dynamic
     *              info/blue ("info" or "blue" will give you a blue progress bar)
     *              success/green
     *              warning/yellow
     *              danger/red
     *              dark/black
     *              (see render-item.component.ts -> setProgressColor() in Webui)
     * */
    public InformationProgress setColor( String color ) {
        this.color = color;
        return this;
    }

    public InformationProgress setMin( int min ) {
        this.min = min;
        return this;
    }

    public InformationProgress setMax( int max ) {
        this.max = max;
        return this;
    }
}
