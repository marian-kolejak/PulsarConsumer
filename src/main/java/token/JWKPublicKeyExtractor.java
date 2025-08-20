package token;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;

public class JWKPublicKeyExtractor {
    public static void main(String[] args) throws Exception {
        String jwkJson = "{ \"kty\": \"RSA\", \"kid\": \"1ea19b2b-0538-4618-9af0-01b31a9d7e25\", \"use\": \"sig\", \"alg\": \"RSA-OAEP-256\", \"n\": \"qFXio6FFx2ArOiJRRawvp3G2ynxG43Lfz3a-rhFZWbnZYi6VrJwWDaxcw3HTXgccaYP6rru8PVm3-ErMlzwfLdmQrRl2MQdWa4cVXkcv66cWn7U4ZGCMO-AhdFc1REm2CgBh8_CcFr1oQM8Ob9DfG2BYNXlVunUaglYgSaizxnwlOu-A8H0t6WMAqA20Jmtp6jwPUE_NBm4ZOKdVXlfVFFfTdSqvl-ggs0layZTK7FESvQN006yrnPp3aFOVQGM83JCeGb65WlPUnDHEHyiW5t6s1oQ0Q0MzrdFdGW4_ZcwvD-6J8rLiCeyHAZSgb3viJpbPx8oaynkckxfkOAfxqQ\", \"e\": \"AQAB\", \"d\": \"MCEuRb6sps62TU7WkecjUOrQaqCR3CJSH41CA57uPxtGLi5HmSyfmZU4iKEY3_-Efh7AJAmFTr1CPnmUeYE1IPTdysBG7Mm-Scw7rdMIvoXtkAhOcVSxg8UXi54LmtTAeaeWWhrnZhENp8oLxKihwcAQOGnSuxcUV0Osw6K6k_sfSsBYpc4uPE1cPuhC0VRD44oo5LTpHHbNSy9U2E35Lazn5U7Bd62L7U4OOJebylCx9KQHoYWrGyGJWGImcaTsk7_61Lug7Wv5eYoBjoB_ifxVAMPtAHOZuFTMn-N055W4kOUeHtCaqbW2Fc2wc9ZeBT5M0GemoPpVinp3A-44aQ\" }";

        // Parse private JWK
        JWK privateJWK = JWK.parse(jwkJson);

        // Extract public key
        RSAKey publicJWK = ((RSAKey) privateJWK).toPublicJWK();

        // Print public JWK
        System.out.println("Public JWK:");
        System.out.println(publicJWK.toJSONString());
    }
}
