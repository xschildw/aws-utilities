package org.sagebionetworks.aws.utils.s3;

import java.util.EnumSet;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.NotificationConfiguration;
import com.amazonaws.services.s3.model.TopicConfiguration;
import com.amazonaws.services.sns.AmazonSNSClient;

/**
 * Used to establish S3 object notification events to be published to an SNS
 * topic.
 * 
 */
public class BucketListener {

	public BucketListener(AmazonS3Client awsS3Client,
			AmazonSNSClient awsSNClient, BucketListenerConfiguration config) {
		if(awsS3Client == null){
			throw new IllegalArgumentException("AmazonS3Client cannot be null");
		}
		if(awsSNClient == null){
			throw new IllegalArgumentException("AmazonSNSClient cannot be null");
		}
		if(config == null){
			throw new IllegalArgumentException("Configuration cannot be null");
		}
		if(config.getBucketName() == null){
			throw new IllegalArgumentException("Bucket cannot be null");
		}
		if(config.getConfigName() == null){
			throw new IllegalArgumentException("ConfigName cannot be null");
		}
		if(config.getEvent() == null){
			throw new IllegalArgumentException("Event cannot be null");
		}
		if(config.getTopicName() == null){
			throw new IllegalArgumentException("Topic name cannot be null");
		}
		// Ensure the topic exists and get the topic ARN.
		String topicArn = awsSNClient.createTopic(config.getTopicName())
				.getTopicArn();
		
		// Ensure the bucket exists
		awsS3Client.createBucket(config.getBucketName());

		// Is this topic already configured to listen to this bucket?
		NotificationConfiguration notificationConfig = awsS3Client
				.getBucketNotificationConfiguration(config.getBucketName())
				.getConfigurationByName(config.getConfigName());
		if (notificationConfig == null) {
			// It does not exists to create it
			awsS3Client.setBucketNotificationConfiguration(config
					.getBucketName(), new BucketNotificationConfiguration()
					.addConfiguration(
							config.getConfigName(),
							new TopicConfiguration(topicArn, EnumSet.of(config
									.getEvent()))));
		}
	}

}
