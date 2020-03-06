package com.ecs.demo.bucket;

public abstract class Bucket {
	 
    private final BucketType bucketType;
    private int size;
    private String name;
	 
    public Bucket(BucketType bucketType) {
        this.bucketType = bucketType;
    }

    public BucketType getBucketType() {
        return bucketType;
    }

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
    
    @Override
    public String toString() {
    	return String.format("Bucket of: %d Objects", size); 
    }
}
