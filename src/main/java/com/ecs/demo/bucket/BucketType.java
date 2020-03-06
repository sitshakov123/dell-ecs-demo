package com.ecs.demo.bucket;

public enum BucketType {
    TINY("Tiny"),
	SMALL("Small"),
    MEDIUM("Medium"),
    LARGE("Large");

	private String value;

	private BucketType(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return this.value;
	}

}
