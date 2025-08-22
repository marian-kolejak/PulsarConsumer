package utils;

import java.util.List;

public class MessageSaver {
    public static void writeMessages(List<String> messages, String outputFilePath) {
        // Write messages to the output file in JSON format.
        MessageWriter.writeMessagesToFile(messages, outputFilePath, messages.size(), true);
    }
}
