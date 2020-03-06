package com.ecs.demo.factory;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;

import org.apache.commons.codec.binary.Base64;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;

/**
 * Factory class to create the ECS S3 client.  The client will be used in the examples for the
 * Java ECS S3 interface.
 */
public class ECSS3Factory {

    /* the S3 access key id - this is equivalent to the user */
    public static final String S3_ACCESS_KEY_ID = "";

    /* the S3 secret key associated with the S3_ACCESS_KEY_ID */
    public static final String S3_SECRET_KEY = "";

    /* the end point of the ECS S3 REST interface */
    public static final String S3_URI = "";

    public static S3Client getS3Client() throws URISyntaxException {
        // for client-side load balancing
        //S3Config config = new S3Config(Protocol.HTTPS, S3_HOST1, S3_HOST2);
        // ditto with multiple VDCs
        //S3Config config = new S3Config(Protocol.HTTPS, new Vdc(S3_V1_HOST), new Vdc(S3_V2_HOST));

        S3Config config = new S3Config(new URI(S3_URI));

        config.withIdentity(S3_ACCESS_KEY_ID).withSecretKey(S3_SECRET_KEY);

        S3Client client = new S3JerseyClient(config);

        return client;
    }

}