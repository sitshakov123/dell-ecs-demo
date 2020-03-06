package com.ecs.demo.bucket;

public class BucketFactory {
	
    public static Bucket getBucket(BucketType bucketType, String name) {
    	Bucket bucket = null;
        
        switch (bucketType) {
        case TINY:
        	bucket = new TinyBucket();
        	bucket.setSize(1000);
            break;
 
        case SMALL:
        	bucket = new SmallBucket();
        	bucket.setSize(10000);
            break;
 
        case MEDIUM:
        	bucket = new MediumBucket();
        	bucket.setSize(100000);
            break;
            
        case LARGE:
        	bucket = new MediumBucket();
        	bucket.setSize(1000000);
            break;
 
        default:
            // throw some exception
            break;
        }
        
        bucket.setName(name);
        
        return bucket;
    }
}
