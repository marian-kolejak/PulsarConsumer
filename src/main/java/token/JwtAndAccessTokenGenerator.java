package token;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.JWTClaimsSet;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

public class JwtAndAccessTokenGenerator {

    private static final Logger log = LoggerFactory.getLogger(JwtAndAccessTokenGenerator.class);

    public static String generateAccessToken() throws Exception {
        ClassLoader classLoader = JwtAndAccessTokenGenerator.class.getClassLoader();

        String jwkJson;
        try (InputStream inputStream = classLoader.getResourceAsStream("private-key-test.jwk.json")) {
            if (inputStream == null) {
                log.error("❌ Súbor private-key-test.jwk.json nebol nájdený v resources.");
                throw new IllegalArgumentException("Súbor private-key-test.jwk.json neexistuje.");
            }

            jwkJson = new Scanner(inputStream, StandardCharsets.UTF_8).useDelimiter("\\A").next();
        } catch (Exception e) {
            log.error("❌ Chyba pri načítaní JWK súboru", e);
            throw e;
        }

        RSAKey rsaKey;
        try {
            JWK jwk = JWK.parse(jwkJson);
            rsaKey = jwk.toRSAKey();
        } catch (Exception e) {
            log.error("❌ Chyba pri parsovaní JWK na RSAKey", e);
            throw e;
        }

        String jwt;
        try {
            JWSSigner signer = new RSASSASigner(rsaKey);
            Instant now = Instant.now();

            JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                    .audience("https://idp.rbinternational.com/as/token.oauth2")
                    .subject("dc-0ujcfy5vn10ory5sxv5xk81jv")
                    .issuer("dc-0ujcfy5vn10ory5sxv5xk81jv")
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
            log.debug("🔐 JWT vygenerovaný: {}", jwt);
        } catch (Exception e) {
            log.error("❌ Chyba pri generovaní alebo podpise JWT", e);
            throw e;
        }

        String body;
        try {
            body = "grant_type=client_credentials"
                    + "&scope=m2m"
                    + "&client_assertion_type=" + URLEncoder.encode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer", StandardCharsets.UTF_8)
                    + "&client_assertion=" + URLEncoder.encode(jwt, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("❌ Chyba pri tvorbe tela požiadavky", e);
            throw e;
        }

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://idp.rbinternational.com/as/token.oauth2"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            log.debug("📨 Odpoveď z token endpointu: {}", response.body());

            if (response.statusCode() != 200) {
                log.error("❌ Token endpoint vrátil chybu: {} - {}", response.statusCode(), response.body());
                throw new RuntimeException("Token endpoint vrátil chybu: " + response.statusCode());
            }

            String responseBody = response.body();
            int start = responseBody.indexOf("\"access_token\":\"") + 16;
            int end = responseBody.indexOf("\"", start);
            String accessToken = responseBody.substring(start, end);

            log.info("✅ Access token úspešne získaný.");
            return accessToken;

        } catch (Exception e) {
            log.error("❌ Chyba pri odosielaní požiadavky na token endpoint", e);
            throw e;
        }
    }
}

