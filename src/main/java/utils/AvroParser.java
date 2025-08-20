package utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;

public class AvroParser {
    public static String parse(byte[] bytes, String pojoSchema) throws Exception {
        try {
            Schema.Parser parser = new Schema.Parser();
            parser.setValidate(true);
            Schema schema = parser.parse(pojoSchema);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericData.Record record = reader.read(null, decoder);
            return record.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing message", e);
        }
    }
}
