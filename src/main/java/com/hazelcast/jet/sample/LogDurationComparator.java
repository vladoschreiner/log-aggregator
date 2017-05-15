package com.hazelcast.jet.sample;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.impl.distributed.DistributedComparators;

public class LogDurationComparator implements Distributed.Comparator<Log> {

    public static final Distributed.Comparator<Log> DURATION_COMPARATOR = new LogDurationComparator();

    @Override
    public int compare(Log c1, Log c2) {
        return Integer.compare(c1.getDuration(), c2.getDuration());
    }

}
