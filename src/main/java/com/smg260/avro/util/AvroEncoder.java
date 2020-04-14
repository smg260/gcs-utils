package com.smg260.avro.util;

import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.util.Iterator;

public class AvroEncoder<T> {

    private final SpecificDatumWriter<T> writer ;
    private final Schema schema;

    private static ThreadLocal<BinaryEncoder> localEncoder = new ThreadLocal<>();

    public AvroEncoder(Schema schema) {
        this.writer = new SpecificDatumWriter<>(schema);
        this.schema = schema;
    }

    public ByteString encode(T record) throws IOException {
        ByteString.Output out = ByteString.newOutput();
        if(localEncoder.get() == null) {
            localEncoder.set(EncoderFactory.get().binaryEncoder(out, null));
        }
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, localEncoder.get());

        writer.write(record, encoder);
        encoder.flush();
        return out.toByteString();
    }

    public Iterator<T> generateRandom(int n) {
        return new RandomData<T>(schema, n).iterator();
    }
}
