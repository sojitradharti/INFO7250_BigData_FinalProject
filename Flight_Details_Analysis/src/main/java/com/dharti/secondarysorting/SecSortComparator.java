package com.dharti.secondarysorting;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortComparator extends WritableComparator {
    public SecSortComparator() {
        super(CompositeKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b)
    {
        CompositeKey ck1 = (CompositeKey)a;
        CompositeKey ck2 = (CompositeKey)b;

        int result = ck1.getZip().compareTo(ck2.getZip());

        if(result==0)
        {
            return -1*ck1.getBikeId().compareTo(ck2.getBikeId());
        }

        return result;
    }
}

