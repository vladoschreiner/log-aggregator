package com.hazelcast.jet.sample;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.windowing.Frame;

import java.util.Map;

public class LogDurationFrameComparator implements Distributed.Comparator<Frame<String, Map.Entry<String, Double>>> {

    public static final Distributed.Comparator<Frame<String, Map.Entry<String, Double>>> DURATION_FRAME_COMPARATOR =
            new LogDurationFrameComparator();

    @Override
    public int compare(Frame<String, Map.Entry<String, Double>> o1, Frame<String, Map.Entry<String, Double>> o2) {
        return Double.compare(o1.getValue().getValue(), o2.getValue().getValue());
    }
}
