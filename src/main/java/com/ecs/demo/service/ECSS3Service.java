package com.ecs.demo.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;

import com.ecs.demo.bucket.Bucket;
import com.ecs.demo.factory.ECSS3Factory;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.AbstractVersion;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.ListObjectsRequest;

public class ECSS3Service {

	private final S3Client client;
	
	public ECSS3Service() throws URISyntaxException {
		this.client = ECSS3Factory.getS3Client();
	}
	
	public void createBucket(Bucket bucket) {
		String bucketName = bucket.getName();
		
		// create the bucket
		client.createBucket(bucketName);
	}
	
	public void deleteBucket(Bucket bucket) {
		String bucketName = bucket.getName();

		// delete the bucket content
        if (client.getBucketVersioning(bucketName).getStatus() != null) {
        	// delete all object versions
            for (AbstractVersion version : client.listVersions(bucketName, null).getVersions()) {
            	client.deleteVersion(bucketName, version.getKey(), version.getVersionId());
            }
        } else {
        	// delete all objects
            for (S3Object object : client.listObjects(bucketName).getObjects()) {
            	client.deleteObject(bucketName, object.getKey());
            }
        }
        
        // delete the bucket
        client.deleteBucket(bucketName);
	}
	
	public void createObject(Bucket bucket, String key, String content) throws Exception {
		String bucketName = bucket.getName();

		// create the object in the bucket
        client.putObject(bucketName, key, content.getBytes("UTF-8"), null);
	}
	
	public String readObject(Bucket bucket, String key) {
		String bucketName = bucket.getName();

        // read the object from the demo bucket
        String content = client.readObject(bucketName, key, String.class);

        return content;
	}
	
	public S3Object findObject(Bucket bucket, String key) {
		String bucketName = bucket.getName();
		S3Object foundObject = null;

        ListObjectsRequest lor = new ListObjectsRequest(bucketName);
        if(key!=null && !key.isEmpty()) {
            lor.setPrefix(key);
        }

        ListObjectsResult res = client.listObjects(lor);
        List<S3Object> objects = res.getObjects();
        
        if (objects.size() > 0) {
            foundObject = objects.get(0);
        }
        
		return foundObject;
   	}

	public GetObjectResult<InputStream> getObject(Bucket bucket, String key) {
		String bucketName = bucket.getName();

        ListObjectsRequest lor = new ListObjectsRequest(bucketName);
        if(key!=null && !key.isEmpty()) {
            lor.setPrefix(key);
        }

        return client.getObject(bucket.getName(), key);
   	}
	
	public List<S3Object> findObjects(Bucket bucket, String path) {
		String bucketName = bucket.getName();

        ListObjectsRequest lor = new ListObjectsRequest(bucketName);
        if(path!=null && !path.isEmpty()) {
            lor.setPrefix(path);
        }

        ListObjectsResult res = client.listObjects(lor);
        return res.getObjects();
   	}
	
	public void listObjects(Bucket bucket, String prefix, String delimiter, String marker, String maxKeys) throws Exception {
		String bucketName = bucket.getName();

		while(true) {
//            System.out.println( "Enter the prefix (empty for none):" );
//            String prefix = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
//            System.out.println( "Enter the delimiter (e.g. /, empty for none)" );
//            String delimiter = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
//            System.out.println( "Enter the marker (empty for none)" );
//            String marker = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
//            System.out.println( "Enter the max keys (empty for default)" );
//            String maxKeys = new BufferedReader( new InputStreamReader( System.in ) ).readLine();

            ListObjectsRequest lor = new ListObjectsRequest(bucketName);
            if(!prefix.isEmpty()) {
                lor.setPrefix(prefix);
            }
            if(!delimiter.isEmpty()) {
                lor.setDelimiter(delimiter);
            }
            if(!marker.isEmpty()) {
                lor.setMarker(marker);
            }
            if(!maxKeys.isEmpty()) {
                lor.setMaxKeys(new Integer(maxKeys));
            }

            ListObjectsResult res = client.listObjects(lor);
            System.out.println("-----------------");
            System.out.println("Bucket: " + res.getBucketName());
            System.out.println("Prefix: " + res.getPrefix());
            System.out.println("Delimiter: " + res.getDelimiter());
            System.out.println("Marker: " + res.getMarker());
            System.out.println("IsTruncated? " + res.isTruncated());
            System.out.println("NextMarker: " + res.getNextMarker());
            System.out.println();
            if(res.getCommonPrefixes() != null) {
                for(String s : res.getCommonPrefixes()) {
                    System.out.println("CommonPrefix: " + s);
                }
            }
            System.out.printf("%30s %10s %s\n", "LastModified", "Size", "Key");
            System.out.println("------------------------------ ---------- ------------------------------------------");
            for(S3Object obj : res.getObjects()) {
                System.out.printf("%30s %10d %s\n", obj.getLastModified().toString(), obj.getSize(), obj.getKey());
            }
            
            System.out.println( "Another? (Y/N) " );
            String another = new BufferedReader( new InputStreamReader( System.in ) ).readLine();

            if(!another.toUpperCase().equals("Y")) {
                break;
            }
        }
	}
}
