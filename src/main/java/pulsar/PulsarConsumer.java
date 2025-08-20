package pulsar;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import token.JwtAndAccessTokenGenerator;
import utils.MessageWriter;
import utils.SchemaLoader;
import utils.TopicAliasResolver;
import utils.PropertyLoader;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static utils.AvroParser.parse;

public class PulsarConsumer {
    private static final Logger log = LoggerFactory.getLogger(PulsarConsumer.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("🛑 Shutdown signal received. Ukončujem aplikáciu...");
            running = false;
        }));

        // 🕒 Logovanie časového pásma JVM
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

        Properties configProps = PropertyLoader.load("config.properties");
        if (configProps == null) return;

        // 🧾 Výpis všetkých relevantných properties
        for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
            log.info("⚙️ Property: {} = {}", entry.getKey(), entry.getValue());
        }

        String regex = configProps.getProperty("regex", ".*");
        String topicInput = configProps.getProperty("topic", "LEAD");
        String environment = configProps.getProperty("environment", "UAT");
        String fromTimestampStr = configProps.getProperty("fromTimestamp");

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

        Properties envProps = PropertyLoader.loadFromResources("env.properties");
        if (envProps == null) return;

        String defaultEnvironment = "UAT";
        String serviceUrl = envProps.getProperty(environment);
        if (serviceUrl == null) {
            log.warn("⚠️ Pre prostredie '{}' neexistuje definovaný serviceUrl. Používam default '{}'.", environment, defaultEnvironment);
            serviceUrl = envProps.getProperty(defaultEnvironment);
            if (serviceUrl == null) {
                log.error("❌ Ani pre defaultné prostredie '{}' neexistuje serviceUrl.", defaultEnvironment);
                return;
            }
            environment = defaultEnvironment;
        }

        String outputFilePath = "output/" + environment + "_" + topicInput + "_From_" + fromTimestampStr + ".json";
        List<String> messages = new ArrayList<>();

        String subscription = "TBSK-HO-pulsar-test-" + UUID.randomUUID();

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .authentication(AuthenticationFactory.token(token))
                .build()) {

            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe()) {

                log.info("📡 Pripojený k topicu: {}", topic);

                // ⏱️ Seek na timestamp, ak je definovaný
                if (fromTimestampStr != null && !fromTimestampStr.isEmpty()) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm");
                        Date fromDate = sdf.parse(fromTimestampStr);
                        long timestampMillis = fromDate.getTime();
                        consumer.seek(timestampMillis);
                        log.info("⏱️ Seekujem na timestamp: {} ({} ms)", fromTimestampStr, timestampMillis);
                    } catch (Exception e) {
                        log.warn("⚠️ Nepodarilo sa parsovať fromTimestamp '{}'", fromTimestampStr, e);
                    }
                }

                log.info("⏳ Čakám na správy...");

                while (running) {
                    try {
                        Message<byte[]> msg = consumer.receive(5000, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (msg == null) {
                            log.debug("⏱️ Timeout pri čakaní na správu.");
                            continue;
                        }

                        String parsed = parse(msg.getData(), pojoSchema);
                        Matcher matcher = pattern.matcher(parsed);

                        if (matcher.find()) {
                            messages.add(parsed);
                            MessageWriter.writeMessagesToFile(messages, outputFilePath);
                            log.info("✅ Zhodná správa: {}", parsed);
                        } else {
                            log.debug("⏭️ Ignorovaná správa: {}", parsed);
                        }

                        consumer.acknowledge(msg);
                    } catch (Exception e) {
                        log.error("⚠️ Chyba pri spracovaní správy", e);
                    }
                }

            }

        } catch (PulsarClientException e) {
            log.error("❌ Chyba pri práci s Pulsar klientom", e);
        }

        log.info("✅ Aplikácia ukončená.");
    }
}