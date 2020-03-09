package com.ecs.demo.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.ecs.demo.bucket.Bucket;
import com.ecs.demo.factory.ECSS3Factory;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.AbstractVersion;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ListVersionsResult;
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
        	ListVersionsResult result = client.listVersions(bucketName, null);
        	while (true) {
	            for (AbstractVersion version : result.getVersions()) {
	            	client.deleteVersion(bucketName, version.getKey(), version.getVersionId());
	            }

	            if (!result.isTruncated()) break;
	            
	            result = client.listMoreVersions(result);
        	}
        } else {
        	// delete all objects
        	ListObjectsResult result = client.listObjects(bucketName);
        	while (true) {
	            for (S3Object object : result.getObjects()) {
	            	client.deleteObject(bucketName, object.getKey());
	            }

	            if (!result.isTruncated()) break;
	            
	            result = client.listMoreObjects(result);
        	}
        }
        
        // delete the bucket
        //client.deleteBucket(bucketName);
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
		List<S3Object> returnList = new ArrayList<>();

        ListObjectsRequest lor = new ListObjectsRequest(bucketName);
        if(path!=null && !path.isEmpty()) {
            lor.setPrefix(path);
        }
        
        ListObjectsResult result = client.listObjects(lor);
    	while (true) {
            returnList.addAll(result.getObjects());

            if (!result.isTruncated()) break;
            
            result = client.listMoreObjects(result);
    	}
        return returnList;
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

            int totalObjects = 0;
            ListObjectsResult result = client.listObjects(lor);
        	while (true) {
        		totalObjects += result.getObjects().size();
        		
	            System.out.println("-----------------");
	            System.out.println("Bucket: " + result.getBucketName());
	            System.out.println("Prefix: " + result.getPrefix());
	            System.out.println("Delimiter: " + result.getDelimiter());
	            System.out.println("Marker: " + result.getMarker());
	            System.out.println("IsTruncated? " + result.isTruncated());
	            System.out.println("NextMarker: " + result.getNextMarker());
	            System.out.println();
	            if(result.getCommonPrefixes() != null) {
	                for(String s : result.getCommonPrefixes()) {
	                    System.out.println("CommonPrefix: " + s);
	                }
	            }
	            System.out.printf("%30s %10s %s\n", "LastModified", "Size", "Key");
	            System.out.println("------------------------------ ---------- ------------------------------------------");
	            for(S3Object obj : result.getObjects()) {
	                System.out.printf("%30s %10d %s\n", obj.getLastModified().toString(), obj.getSize(), obj.getKey());
	            }
	            
	            if (!result.isTruncated()) break;
	            
	            result = client.listMoreObjects(result);
        	}
        	
        	System.out.println("\nTotal objects: " + totalObjects + "\n\n");
            
            System.out.println( "Another? (Y/N) " );
            String another = new BufferedReader( new InputStreamReader( System.in ) ).readLine();

            if(!another.toUpperCase().equals("Y")) {
                break;
            }
        }
	}
}
