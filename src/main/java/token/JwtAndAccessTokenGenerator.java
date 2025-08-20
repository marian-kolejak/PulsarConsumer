package token;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.JWTClaimsSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Scanner;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.PropertyLoader;
import java.util.Properties;

public class JwtAndAccessTokenGenerator {

    private static final Logger log = LoggerFactory.getLogger(JwtAndAccessTokenGenerator.class);

    // Property key constants
    private static final String PROP_TOKEN_ENDPOINT = "TOKEN_ENDPOINT";
    private static final String PROP_AUDIENCE = "AUDIENCE";
    private static final String PROP_SUBJECT = "SUBJECT";
    private static final String PROP_ISSUER = "ISSUER";
    private static final String PROP_SCOPE = "SCOPE";
    private static final String PROP_GRANT_TYPE = "GRANT_TYPE";
    private static final String PROP_CLIENT_ASSERTION_TYPE = "CLIENT_ASSERTION_TYPE";
    private static final String PROP_JWK_RESOURCE = "JWK_RESOURCE";

    static Properties props = PropertyLoader.load("env.properties");
    static String tokenEndpoint = props.getProperty(PROP_TOKEN_ENDPOINT);
    static String audience = props.getProperty(PROP_AUDIENCE);
    static String subject = props.getProperty(PROP_SUBJECT);
    static String issuer = props.getProperty(PROP_ISSUER);
    static String scope = props.getProperty(PROP_SCOPE);
    static String grantType = props.getProperty(PROP_GRANT_TYPE);
    static String clientAssertionType = props.getProperty(PROP_CLIENT_ASSERTION_TYPE);
    static String jwkResource = props.getProperty(PROP_JWK_RESOURCE);

    static {
        if (tokenEndpoint == null || audience == null || subject == null ||
                issuer == null || scope == null || grantType == null ||
                clientAssertionType == null || jwkResource == null) {
            log.error("❌ Missing required property in env.properties.");
            throw new RuntimeException(new TokenGenerationException("One or more required properties are missing in env.properties."));
        }
    }

    public static String generateAccessToken() throws TokenGenerationException {
        ClassLoader classLoader = JwtAndAccessTokenGenerator.class.getClassLoader();

        String jwkJson;
        try (InputStream inputStream = classLoader.getResourceAsStream(jwkResource);
             Scanner scanner = inputStream != null ? new Scanner(inputStream, StandardCharsets.UTF_8) : null) {
            if (inputStream == null || scanner == null) {
                log.error("❌ File {} not found in resources.", jwkResource);
                throw new TokenGenerationException(String.format("File %s does not exist.", jwkResource));
            }
            if (!scanner.hasNext()) {
                log.error("❌ JWK file {} is empty.", jwkResource);
                throw new TokenGenerationException(String.format("JWK file %s is empty.", jwkResource));
            }
            jwkJson = scanner.useDelimiter("\\A").next();
        } catch (IOException e) {
            log.error("❌ Error reading JWK file", e);
            throw new TokenGenerationException("Error reading JWK file", e);
        }

        RSAKey rsaKey;
        try {
            JWK jwk = JWK.parse(jwkJson);
            rsaKey = jwk.toRSAKey();
        } catch (ParseException e) {
            log.error("❌ Error parsing JWK to RSAKey", e);
            throw new TokenGenerationException("Error parsing JWK to RSAKey", e);
        }

        String jwt;
        try {
            JWSSigner signer = new RSASSASigner(rsaKey);
            Instant now = Instant.now();

            JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                    .audience(audience)
                    .subject(subject)
                    .issuer(issuer)
                    .jwtID(UUID.randomUUID().toString())
                    .issueTime(Date.from(now))
                    .expirationTime(Date.from(now.plusSeconds(3600)))
                    .build();

            SignedJWT signedJWT = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).build(),
                    claimsSet
            );

            signedJWT.sign(signer);
            jwt = signedJWT.serialize();
            log.debug("🔐 JWT generated successfully (token value not logged).");
        } catch (JOSEException e) {
            log.error("❌ Error generating or signing JWT", e);
            throw new TokenGenerationException("Error generating or signing JWT", e);
        }

        String body;
        try {
            body = "grant_type=" + URLEncoder.encode(grantType, StandardCharsets.UTF_8)
                    + "&scope=" + URLEncoder.encode(scope, StandardCharsets.UTF_8)
                    + "&client_assertion_type=" + URLEncoder.encode(clientAssertionType, StandardCharsets.UTF_8)
                    + "&client_assertion=" + URLEncoder.encode(jwt, StandardCharsets.UTF_8);
        } catch (NullPointerException | IllegalArgumentException e) {
            log.error("❌ Error creating request body", e);
            throw new TokenGenerationException("Error creating request body", e);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("❌ Error sending HTTP request", e);
            throw new TokenGenerationException("Error sending HTTP request", e);
        }

        log.debug("📨 Response from token endpoint: {}", response.body());

        if (response.statusCode() != 200) {
            log.error("❌ Token endpoint returned error: {} - {}", response.statusCode(), response.body());
            throw new TokenGenerationException(
                    String.format("Token endpoint error: %d - %s", response.statusCode(), response.body()));
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(response.body());
        } catch (IOException e) {
            log.error("❌ Error parsing token endpoint response", e);
            throw new TokenGenerationException("Error parsing token endpoint response", e);
        }
        String accessToken = jsonNode.has("access_token") ? jsonNode.get("access_token").asText() : null;
        if (accessToken == null) {
            log.error("❌ Access token not found in response: {}", response.body());
            throw new TokenGenerationException("Access token not found in response.");
        }

        log.info("✅ Access token obtained successfully (token value not logged).");
        return accessToken;
    }
}