package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class MessageWriter {
    private static final Logger log = LoggerFactory.getLogger(MessageWriter.class);
    private static final String OUTPUT_DIR = "output";

    public static void writeMessagesToFile(List<String> messages, String outputFilePath) {
        try {
            // Vytvorenie adresára, ak neexistuje
            File directory = new File(OUTPUT_DIR);
            if (!directory.exists() && !directory.mkdirs()) {
                log.error("❌ Nepodarilo sa vytvoriť adresár: {}", directory.getAbsolutePath());
                return;
            }

            // Zápis do súboru pomocou BufferedWriter
            File outputFile = new File(outputFilePath);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                writer.write("{\"messages\": [\n");
                for (int i = 0; i < messages.size(); i++) {
                    writer.write(messages.get(i));
                    log.info("📝 Zapisujem správu do súboru: {}", messages.get(i));
                    if (i < messages.size() - 1) {
                        writer.write(",\n");
                    }
                }
                writer.write("\n]}");
            }

            log.info("📁 Správy uložené do súboru: {}", outputFile.getAbsolutePath());

        } catch (IOException e) {
            log.error("❌ Chyba pri zápise do súboru", e);
        }
    }
}
