package utils;

/**
 * Centralized constants for configuration, file names, formats, and other repeated strings.
 */
public final class Constants {
    // Property file names
    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final String ENV_PROPERTIES = "env.properties";

    // Date format used for parsing timestamps
    public static final String DATE_FORMAT = "yyyy-MM-dd_HH-mm";

    // Output directory and file suffix
    public static final String OUTPUT_DIR = "output";
    public static final String OUTPUT_FILE_SUFFIX = ".json";

    // Subscription name prefix for Pulsar
    public static final String SUBSCRIPTION_PREFIX = "TBSK-HO-pulsar-test-";

    // Add more constants as needed, e.g.:
    // public static final String TOPIC_CAMPAIGN = "CAMPAIGN";
    // public static final String SOME_OTHER_CONSTANT = "value";

    private Constants() {
        // Prevent instantiation
    }
}