package com.dharti.secondarysorting;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortGroupComparator extends WritableComparator {
    public SecSortGroupComparator() {
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        CompositeKey ck1 = (CompositeKey)a;
        CompositeKey ck2 = (CompositeKey)b;

        return ck1.getZip().compareTo(ck2.getZip());
    }

}
