package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageWriter {
    private static final Logger log = LoggerFactory.getLogger(MessageWriter.class);
    private static final AtomicInteger totalMessagesWritten = new AtomicInteger(0);

    public static void writeMessagesToFile(List<String> messages, String outputFilePath, int batchSize, boolean isLastBatch) {
        File outputFile = new File(outputFilePath);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("{\"messages\": [\n");
            for (int i = 0; i < messages.size(); i++) {
                writer.write(messages.get(i));
                if ((i + 1) % 100 == 0 || i == messages.size() - 1) {
                    log.info("🔢 Written {} messages...", i + 1);
                }
                if (i < messages.size() - 1) {
                    writer.write(",\n");
                }
            }
            writer.write("\n]}");
            totalMessagesWritten.addAndGet(batchSize);
            if (isLastBatch) {
                log.info("🔢 Written {} messages...", totalMessagesWritten.get());
                log.info("📁 Správy uložené do súboru: {}", outputFile.getAbsolutePath());
            }
        } catch (IOException e) {
            log.error("❌ Chyba pri zápise do súboru", e);
        }
    }
}