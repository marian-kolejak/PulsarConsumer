package pulsar;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import token.JwtAndAccessTokenGenerator;
import utils.Constants;
import utils.SchemaLoader;
import utils.TopicAliasResolver;
import utils.PropertyLoader;
import utils.MessageSaver;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;

import static utils.AvroParser.parse;

public class PulsarConsumer {
    private static final Logger log = LoggerFactory.getLogger(PulsarConsumer.class);
    private static volatile boolean running = true;
    private static volatile boolean messagesWritten = false;

    public static void main(String[] args) {
        AtomicReference<List<String>> messages = new AtomicReference<>(new ArrayList<>());
        final String[] outputFilePath = new String[1];

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("🛑 Shutdown signal received. Ukončujem aplikáciu...");
            running = false;
            if (!messages.get().isEmpty() && outputFilePath[0] != null && !messagesWritten) {
                ensureOutputDirectoryExists(Constants.OUTPUT_DIR);
                MessageSaver.writeMessages(messages.get(), outputFilePath[0]);
                log.info("📝 Zapísaných {} správ do súboru: {}", messages.get().size(), outputFilePath[0]);
                messagesWritten = true;
            }

            if (!messagesWritten) {
                log.info("📭 Žiadne správy neboli nájdené v topiku.");
            }
        }));

        TimeZone tz = TimeZone.getDefault();
        log.info("🕒 JVM časové pásmo: {}", tz.getID());
        log.info("🕒 Aktuálny čas JVM: {}", new Date());
        log.info("🕒 JVM ZoneId: {}", ZoneId.systemDefault());

        String token;
        try {
            token = JwtAndAccessTokenGenerator.generateAccessToken();
        } catch (Exception e) {
            log.error("❌ Nepodarilo sa vygenerovať access token", e);
            return;
        }

        log.info("📥 Počet odovzdaných argumentov: {}", args.length);
        for (int i = 0; i < args.length; i++) {
            log.info("📌 Argument [{}]: {}", i, args[i]);
        }

        Properties configProps = validateAndLoadConfig(Constants.CONFIG_PROPERTIES);
        if (configProps == null) return;

        String regex = configProps.getProperty("regex", ".*");
        String topicInput = configProps.getProperty("topic");
        String environment = configProps.getProperty("environment");
        String fromTimestampStr = configProps.getProperty("fromTimestamp");
        boolean waitForNextMessageAfterAllRead = configProps.getProperty("waitForNextMessageAfterAllRead").equals("true");

        Pattern pattern;
        try {
            pattern = Pattern.compile(regex);
        } catch (Exception e) {
            log.error("❌ Neplatný regulárny výraz: {}", regex, e);
            return;
        }

        String topic = TopicAliasResolver.resolve(topicInput);
        log.info("📡 Resolvovaný topic: {}", topic);

        String pojoSchema;
        try {
            pojoSchema = SchemaLoader.loadSchema(topicInput);
        } catch (RuntimeException e) {
            log.error("❌ Chyba pri načítaní Avro schémy pre alias '{}'", topicInput, e);
            return;
        }

        Properties envProps = PropertyLoader.loadFromResources(Constants.ENV_PROPERTIES);
        if (envProps == null) {
            log.error("❌ Nepodarilo sa načítať environment properties.");
            return;
        }

        String serviceUrl = envProps.getProperty(environment);
        if (serviceUrl == null) {
            log.error("❌ Pre prostredie '{}' neexistuje definovaný serviceUrl.", environment);
            return;
        }

        String safeRegex = regex.replaceAll("[^a-zA-Z0-9_\\-]", "");
        log.info("⚙️ safeRegex: {}", safeRegex);
        outputFilePath[0] = Constants.OUTPUT_DIR + "/" + environment + "_" + topicInput + "_From_" + fromTimestampStr + "_Regex_" + safeRegex + Constants.OUTPUT_FILE_SUFFIX;
        ensureOutputDirectoryExists(Constants.OUTPUT_DIR);

        String subscription = Constants.SUBSCRIPTION_PREFIX + UUID.randomUUID();

        try (PulsarClient client = setupPulsarClient(serviceUrl, token);
             Consumer<byte[]> consumer = client.newConsumer()
                     .topic(topic)
                     .subscriptionName(subscription)
                     .subscriptionType(SubscriptionType.Exclusive)
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .subscribe()) {

            log.info("📡 Pripojený k topicu: {}", topic);

            if (fromTimestampStr != null && !fromTimestampStr.isEmpty()) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_FORMAT);
                    Date fromDate = sdf.parse(fromTimestampStr);
                    long timestampMillis = fromDate.getTime();
                    consumer.seek(timestampMillis);
                    log.info("⏱️ Setujem fromTimestampStr: {} ({} ms)", fromTimestampStr, timestampMillis);
                } catch (Exception e) {
                    log.warn("⚠️ Nepodarilo sa parsovať fromTimestamp '{}'", fromTimestampStr, e);
                }
            }

            log.info("⏳ Čakám na správy...");

            messages.set(consumeMessages(consumer, pattern, pojoSchema, waitForNextMessageAfterAllRead));

        } catch (PulsarClientException e) {
            log.error("❌ Chyba pri práci s Pulsar klientom", e);
        }

        if (!messages.get().isEmpty() && outputFilePath[0] != null && !messagesWritten) {
            ensureOutputDirectoryExists(Constants.OUTPUT_DIR);
            MessageSaver.writeMessages(messages.get(), outputFilePath[0]);
            log.info("📝 Zapísaných {} správ do súboru: {}", messages.get().size(), outputFilePath[0]);
            messagesWritten = true;
        }

        if (!messagesWritten) {
            log.info("📭 Žiadne správy neboli nájdené v topiku.");
        }

        log.info("✅ Aplikácia ukončená.");
    }

    private static PulsarClient setupPulsarClient(String serviceUrl, String token) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .authentication(AuthenticationFactory.token(token))
                .build();
    }

    private static List<String> consumeMessages(Consumer<byte[]> consumer, Pattern pattern, String pojoSchema, boolean waitForNextMessageAfterAllRead) {
        List<String> messages = new ArrayList<>();
        int maxRetries = 3;
        int retryCount = 0;
        while (running) {
            try {
                Message<byte[]> msg = consumer.receive(5000, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (msg == null) {
                    log.info("⏱️ Timeout pri čakaní na správu.");
                    if (!waitForNextMessageAfterAllRead) {
                        log.info("🛑 Premenná waitForNextMessageAfterAllRead je false – ukončujem čítanie.");
                        break;
                    }
                    continue;
                }

                String parsed = parse(msg.getData(), pojoSchema);
                Matcher matcher = pattern.matcher(parsed);

                if (matcher.find()) {
                    messages.add(parsed);
                    log.info("✅ Zhodná správa: {}", parsed);
                } else {
                    log.debug("⏭️ Ignorovaná správa: {}", parsed);
                }

                consumer.acknowledge(msg);
                retryCount = 0; // reset on success
            } catch (PulsarClientException.TimeoutException e) {
                log.warn("Timeout while receiving message, attempt {}", retryCount + 1);
                if (++retryCount >= maxRetries) {
                    log.error("Max retries reached, aborting.");
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    log.warn("Sleep interrupted during retry, shutting down.", ie);
                    running = false;
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (PulsarClientException e) {
                log.error("Pulsar client error", e);
                break;
            } catch (ParseException e) {
                log.error("Failed to parse message", e);
            } catch (Exception e) {
                log.error("Unexpected error", e);
                break;
            }
        }
        return messages;
    }

    private static Properties validateAndLoadConfig(String configPath) {
        Properties configProps = PropertyLoader.load(configPath);
        if (configProps == null) {
            log.error("❌ Nepodarilo sa načítať konfiguračný súbor: {}", configPath);
            return null;
        }

        for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
            log.info("⚙️ Property: {} = {}", entry.getKey(), entry.getValue());
        }

        String topicInput = configProps.getProperty("topic");
        String environment = configProps.getProperty("environment");
        String regex = configProps.getProperty("regex", "");
        String waitProp = configProps.getProperty("waitForNextMessageAfterAllRead", "false").trim().toLowerCase();

        if (topicInput == null || topicInput.isEmpty()) {
            log.error("Missing required property: topic");
            return null;
        }
        if (environment == null || environment.isEmpty()) {
            log.error("Missing required property: environment");
            return null;
        }
        if (!regex.isEmpty()) {
            try {
                Pattern.compile(regex);
            } catch (Exception e) {
                log.error("Invalid regex: {}", regex, e);
                return null;
            }
        }
        if (!waitProp.isEmpty() && !waitProp.equals("true") && !waitProp.equals("false")) {
            log.error("Invalid value for waitForNextMessageAfterAllRead: {}", waitProp);
            return null;
        }
        configProps.setProperty("waitForNextMessageAfterAllRead", waitProp.isEmpty() ? "false" : waitProp);

        return configProps;
    }

    private static void ensureOutputDirectoryExists(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                log.info("📁 Vytvorený výstupný adresár: {}", dirPath);
            } else {
                log.error("❌ Nepodarilo sa vytvoriť výstupný adresár: {}", dirPath);
            }
        }
    }
}