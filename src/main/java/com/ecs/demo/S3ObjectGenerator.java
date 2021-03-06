package com.ecs.demo;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import com.ecs.demo.bucket.Bucket;
import com.ecs.demo.bucket.BucketFactory;
import com.ecs.demo.bucket.BucketType;
import com.ecs.demo.service.ECSS3Service;
import com.emc.object.s3.bean.S3Object;

public class S3ObjectGenerator {

	private static final String SOURCE_SYSTEM_PREFIX = "SourceSystem";
	private static final String PATH_DELIMITER = "/";
	private static final String FILENAME_DELIMITER = "_";
	private static final String FILENAME_PREFIX = "File";
	private static final String FILENAME_EXTENSION = ".txt";
	private static final String FILE_CONTENT = "";
	private static final int TEST_COUNT = 100;
	private static int[] seqArray = new int[5];
	private static int[] seqPadSize = new int[] { 3, 4, 2, 2, 3 };   // used for padding numeric path sections with zeros

	private static int[] pathLevelSize = new int[] { 2, 2, 12, 30, 1 };    // Generates 1,440 objects. Can be used as TINY bucket
//	private static int[] pathLevelSize = new int[] { 2, 2, 12, 30, 2 };    // Generates 2,880 objects. Can be used as TINY bucket as well
//	private static int[] pathLevelSize = new int[] { 3, 3, 12, 30, 3 };    // Generates 9,720 objects. Can be used as SMALL bucket
//	private static int[] pathLevelSize = new int[] { 7, 7, 12, 30, 7 };    // Generates 123,480 objects. Can be used as MEDIUM bucket
//	private static int[] pathLevelSize = new int[] { 10, 5, 12, 30, 100 }; // Generates 1,800,000 objects. Can be used as LARGE bucket

	private static ECSS3Service service;

	public static void main(String[] args) {
	
		Bucket bucket = BucketFactory.getBucket(BucketType.TINY, getBucketName(BucketType.TINY));
//		Bucket bucket = BucketFactory.getBucket(BucketType.SMALL, getBucketName(BucketType.SMALL));
//		Bucket bucket = BucketFactory.getBucket(BucketType.MEDIUM, getBucketName(BucketType.MEDIUM));
//		Bucket bucket = BucketFactory.getBucket(BucketType.LARGE, getBucketName(BucketType.LARGE));
		
		System.out.println("Executing on bucket: " + bucket.getName());

		try {
			service = new ECSS3Service();
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return;
		}

		//service.createBucket(bucket);
		//generateObjects(bucket);
		//service.deleteBucket(bucket);
		
		runPerformanceTest(bucket);
		
//		S3Object object = service.findObject(bucket, "SourceSystem001/2001/01/01/File001_0001_01_01_001.txt");
//		System.out.println(object==null ? "Object not found" : "Found object: " + object.getKey());
//		
//		List<S3Object> objects = service.findObjects(bucket, "");
//		System.out.println(objects.size()==0 ? "no objects were found" : "Found " + objects.size() + " objects");
//
//		try {
//			service.listObjects(bucket, "", "", "", "");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	private static void runPerformanceTest(Bucket bucket) {
		String path = "";
		String filename = "";
		long totalTime = 0L;
		
		for (int x=0; x<TEST_COUNT; x++) {
			generateRandomFileKey();
			path = generateKeyPath();
			//filename = generateKeyFilename();
			String fullPath = path + filename;
			System.out.print(fullPath);
			
			long startTime = System.currentTimeMillis();
			try {
				 List<S3Object> objects = service.findObjects(bucket, fullPath);
				 System.out.print("     Found objects: " + objects.size());
			} catch (Exception ex) {
				System.out.print("     Object not found");
				
			}
			long finishTime = System.currentTimeMillis();
			
			long elapsedTime = finishTime - startTime;
			totalTime += elapsedTime;
			System.out.println("     " + elapsedTime);
		}
		
		System.out.println("\nAverage Time per object: " + (totalTime / TEST_COUNT));
		
	}

	private static void generateObjects(Bucket bucket) {
		int objectsCreatedCount = 0;
		
		for (seqArray[0] = 0; seqArray[0] < pathLevelSize[0]; seqArray[0]++) {
			for (seqArray[1] = 0; seqArray[1] < pathLevelSize[1]; seqArray[1]++) {
				for (seqArray[2] = 0; seqArray[2] < pathLevelSize[2]; seqArray[2]++) {
					for (seqArray[3] = 0; seqArray[3] < pathLevelSize[3]; seqArray[3]++) {
						for (seqArray[4] = 0; seqArray[4] < pathLevelSize[4]; seqArray[4]++) {
							String path = generateKeyPath();
							String filename = generateKeyFilename();
							String key = path + filename;

							try {
								service.createObject(bucket, key, FILE_CONTENT);
								objectsCreatedCount++;
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							 System.out.println(key);
						}
					}
				}
			}
		}
		
		System.out.println("Total objects created: " + objectsCreatedCount);
	}
	
	private static void generateRandomFileKey() {
		
		Random rand = new Random();
		
		for (int x=0; x<seqArray.length; x++) {
			seqArray[x] = rand.nextInt(pathLevelSize[x]);
		}
	}

	private static void deleteBuckets(Bucket[] buckets) {
		for (Bucket bucket : buckets) {
			service.deleteBucket(bucket);
		}
	}

	private static void createBuckets(Bucket[] buckets) {
		for (Bucket bucket : buckets) {
			service.createBucket(bucket);
		}
	}

	private static String generateKeyPath() {
		return SOURCE_SYSTEM_PREFIX + padWithZeros(seqArray[0] + 1, seqPadSize[0]) + PATH_DELIMITER
				+ padWithZeros(seqArray[1] + 2000, seqPadSize[1]) + PATH_DELIMITER
				+ padWithZeros(seqArray[2] + 1, seqPadSize[2]) + PATH_DELIMITER
				+ padWithZeros(seqArray[3] + 1, seqPadSize[3]) + PATH_DELIMITER;
	}

	private static String generateKeyFilename() {
		return FILENAME_PREFIX + padWithZeros(seqArray[0] + 1, seqPadSize[0]) + FILENAME_DELIMITER
				+ padWithZeros(seqArray[1] + 2000, seqPadSize[1]) + FILENAME_DELIMITER
				+ padWithZeros(seqArray[2] + 1, seqPadSize[2]) + FILENAME_DELIMITER
				+ padWithZeros(seqArray[3] + 1, seqPadSize[3]) + FILENAME_DELIMITER
				+ padWithZeros(seqArray[4] + 1, seqPadSize[4]) + FILENAME_EXTENSION;
	}

	private static String padWithZeros(int number, int size) {
		String paddedString = StringUtils.leftPad("" + number, size, "0");
		return paddedString;
	}

	private static String getBucketName(BucketType bucketType) {
		return bucketType.toString() + "Bucket";
	}

}
