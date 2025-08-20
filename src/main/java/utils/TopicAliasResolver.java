package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TopicAliasResolver {
    private static final Map<String, String> aliases = new HashMap<>();

    static {
        try (InputStream input = TopicAliasResolver.class.getClassLoader().getResourceAsStream("topic-aliases.properties")) {
            if (input == null) {
                System.err.println("⚠️ Súbor topic-aliases.properties sa nenašiel v classpath.");
            } else {
                Properties props = new Properties();
                props.load(input);
                for (String key : props.stringPropertyNames()) {
                    aliases.put(key, props.getProperty(key));
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Chyba pri načítaní aliasov: " + e.getMessage());
        }
    }

    public static String resolve(String input) {
        return aliases.getOrDefault(input, input);
    }
}
