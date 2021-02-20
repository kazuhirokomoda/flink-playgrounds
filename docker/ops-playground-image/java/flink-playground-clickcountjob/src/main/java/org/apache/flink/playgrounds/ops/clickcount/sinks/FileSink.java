package org.apache.flink.playgrounds.ops.clickcount.sinks;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public class FileSink {
    private FileSink() {}

    public static StreamingFileSink<String> buildSink(String outputPath, String bucketingPeriod) {

        StreamingFileSink<String> fileSink = StreamingFileSink
            .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withBucketAssigner(new DateTimeBucketAssigner(bucketingPeriod))
            .build();

        return fileSink;
    }
}