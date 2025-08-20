package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyLoader {
    private static final Logger log = LoggerFactory.getLogger(PropertyLoader.class);

    public static Properties load(String fileName) {
        Properties props = tryLoadFromFile(fileName);
        if (props != null) {
            log.info("✅ Konfiguračný súbor '{}' načítaný z externého umiestnenia.", fileName);
            return props;
        }

        log.warn("⚠️ Externý konfiguračný súbor '{}' sa nepodarilo načítať. Pokus o načítanie z resources...", fileName);
        props = tryLoadFromResources(fileName);
        if (props != null) {
            log.info("✅ Konfiguračný súbor '{}' načítaný z resources.", fileName);
        } else {
            log.error("❌ Konfiguračný súbor '{}' sa nenašiel ani externe, ani v resources.", fileName);
        }

        return props;
    }

    public static Properties loadFromResources(String fileName) {
        Properties props = tryLoadFromResources(fileName);
        if (props != null) {
            log.info("✅ Konfiguračný súbor '{}' načítaný z resources.", fileName);
        } else {
            log.error("❌ Konfiguračný súbor '{}' sa nenašiel v resources.", fileName);
        }
        return props;
    }

    private static Properties tryLoadFromFile(String fileName) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(fileName)) {
            props.load(input);
            return props;
        } catch (IOException e) {
            return null;
        }
    }

    private static Properties tryLoadFromResources(String fileName) {
        Properties props = new Properties();
        try (InputStream input = PropertyLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) return null;
            props.load(input);
            return props;
        } catch (IOException e) {
            return null;
        }
    }
}
