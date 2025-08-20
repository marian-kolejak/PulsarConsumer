package utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SchemaLoader {

    private static final Properties schemaMap = new Properties();
    private static final String SCHEMA_DIRECTORY = "avro/";

    static {
        try (InputStream input = SchemaLoader.class.getClassLoader().getResourceAsStream("schema-aliases.properties")) {
            if (input != null) {
                schemaMap.load(input);
            } else {
                System.err.println("⚠️ Konfiguračný súbor schema-aliases.properties sa nenašiel.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Chyba pri načítaní schema-aliases.properties", e);
        }
    }

    public static String loadSchema(String alias) {
        String fileName = schemaMap.getProperty(alias);
        if (fileName == null) {
            throw new IllegalArgumentException("Alias schémy '" + alias + "' nie je definovaný.");
        }

        String fullPath = SCHEMA_DIRECTORY + fileName;

        try (InputStream schemaStream = SchemaLoader.class.getClassLoader().getResourceAsStream(fullPath)) {
            if (schemaStream == null) {
                throw new RuntimeException("Schéma '" + fullPath + "' sa nenašla v classpath.");
            }
            return new String(schemaStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Chyba pri načítaní schémy: " + fullPath, e);
        }
    }
}
