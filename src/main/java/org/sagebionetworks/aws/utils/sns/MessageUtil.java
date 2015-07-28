package org.sagebionetworks.aws.utils.sns;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class MessageUtil {

	/**
	 * Extract a message body as a String.
	 * This handles the case where messages are forwarded from a topic.
	 * 
	 * @param message
	 * @return
	 */
	public static String extractMessageBodyAsString(Message message) {
		if (message == null) {
			throw new IllegalArgumentException("Message cannot be null");
		}
		try {
			JSONObject object = new JSONObject(message.getBody());
			if (object.has("TopicArn") && object.has("Message")) {
				return object.getString("Message");
			} else {
				throw new IllegalArgumentException();
			}
		} catch (JSONException e) {
			return message.getBody();
		}
	}

	/**
	 * Build a message from a message body and a topicArn
	 * 
	 * @param body
	 * @param topicArn
	 * @return
	 * @throws JSONException
	 */
	public static Message buildMessage(String body, String topicArn) throws JSONException {
		if(body == null) throw new IllegalArgumentException("Message body cannot be null");
		if(topicArn == null) throw new IllegalArgumentException("topicArn cannot be null");
		JSONObject object = new JSONObject();
		object.put("Message", body);
		object.put("TopicArn", topicArn);
		Message message = new Message();
		message.setBody(object.toString());
		return message;
	}
}
