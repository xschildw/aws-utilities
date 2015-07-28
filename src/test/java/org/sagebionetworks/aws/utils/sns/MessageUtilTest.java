package org.sagebionetworks.aws.utils.sns;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;

public class MessageUtilTest {
	private final String body = "<messageBody>\"message body\"</messageBody>";
	private final String topicArn = "topicArn";
	private final char quote = '"';

	@Test
	public void testBuildMessage() throws JSONException {
		Message message = MessageUtil.buildMessage(body, topicArn);
		String expected = "{Body: {"+quote+"Message"+quote+":"+quote+
				"<messageBody>\\"+quote+"message body\\"+quote+"<\\/messageBody>\","
				+quote+"TopicArn"+quote+":"+quote+"topicArn"+quote+
				"},Attributes: {},MessageAttributes: {}}";
		assertEquals(expected, message.toString());
	}

	@Test (expected=IllegalArgumentException.class)
	public void testBuildMessageWithNullBody() throws JSONException {
		MessageUtil.buildMessage(null, topicArn);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testBuildMessageWithNullTopicArn() throws JSONException {
		MessageUtil.buildMessage(body, null);
	}

	@Test
	public void testExtractMessageBodyFromRawMessage() {
		Message message = new Message();
		message.setBody(body);
		assertEquals(body, MessageUtil.extractMessageBodyAsString(message));
	}

	@Test
	public void testRoundTrip() throws JSONException {
		Message message = MessageUtil.buildMessage(body, topicArn);
		assertEquals(body, MessageUtil.extractMessageBodyAsString(message));
	}

	@Test (expected=IllegalArgumentException.class)
	public void testRoundTripWithoutTopicArn() throws JSONException {
		Message message = new Message();
		String messageBody = "{Body: {"+quote+"Message"+quote+":"+quote+
				"<messageBody>\\"+quote+"message body\\"+quote+"<\\/messageBody>\""
				+"},Attributes: {},MessageAttributes: {}}";
		message.setBody(messageBody);
		MessageUtil.extractMessageBodyAsString(message);
	}
}
