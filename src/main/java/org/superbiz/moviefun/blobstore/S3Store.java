package org.superbiz.moviefun.blobstore;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.util.Optional;

public class S3Store implements BlobStore {
    private AmazonS3Client client;
    private String bucket;

    public S3Store(AmazonS3Client client, String bucket) {
        this.client = client;
        this.bucket = bucket;
    }

    @Override
    public void put(Blob blob) throws IOException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(blob.contentType);
        client.putObject(bucket, blob.name, blob.inputStream, objectMetadata);
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        GetObjectRequest request = new GetObjectRequest(bucket, name);
        S3Object object = client.getObject(request);
        Blob blob = new Blob(name, object.getObjectContent(), object.getObjectMetadata().getContentType());
        return Optional.of(blob);
    }

    @Override
    public void deleteAll() {
        client.deleteBucket(bucket);
    }
}
