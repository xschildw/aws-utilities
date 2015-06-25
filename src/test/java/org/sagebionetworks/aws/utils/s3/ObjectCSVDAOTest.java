package org.sagebionetworks.aws.utils.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.sagebionetworks.aws.utils.s3.ExampleObject.SomeEnum;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class ObjectCSVDAOTest {

	private AmazonS3Client mockS3Client;
	private int stackInstanceNumber;
	private String bucketName;
	private Class<ExampleObject> objectClass;
	private String[] headers;
	private ObjectCSVDAO<ExampleObject> dao;

	@Before
	public void setUp() {
		mockS3Client = Mockito.mock(AmazonS3Client.class);
		stackInstanceNumber = 1;
		bucketName = "object.csv.dao.test";
		objectClass = ExampleObject.class;
		headers = new String[]{"aString", "aLong", "aBoolean", "aDouble", "anInteger", "aFloat", "someEnum"};
		dao = new ObjectCSVDAO<ExampleObject>(mockS3Client, stackInstanceNumber, bucketName, objectClass, headers);
	}

	/**
	 * Test write and read methods
	 * @throws Exception
	 */
	@Test
	public void testRoundTrip() throws Exception{
		Long timestamp = System.currentTimeMillis();
		boolean rolling = false;

		// Build up some sample data
		List<ExampleObject> data = buildExampleObjectList(12);
		// call under test.
		String key = dao.write(data, timestamp, rolling);
		// capture results
		ArgumentCaptor<String> bucketCapture = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> keyCapture = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<InputStream> inCapture = ArgumentCaptor.forClass(InputStream.class);
		ArgumentCaptor<ObjectMetadata> metaCapture = ArgumentCaptor.forClass(ObjectMetadata.class);
		verify(mockS3Client).putObject(bucketCapture.capture(), keyCapture.capture(), inCapture.capture(), metaCapture.capture());
		assertEquals(bucketName, bucketCapture.getValue());
		assertEquals(key, keyCapture.getValue());
		// Can we read the results?
		List<ExampleObject> results = dao.readFromStream(inCapture.getValue());
		assertEquals(data, results);
		assertEquals("attachment; filename="+key+";", metaCapture.getValue().getContentDisposition());
		assertEquals("application/x-gzip", metaCapture.getValue().getContentType());
		assertEquals("gzip", metaCapture.getValue().getContentEncoding());
		assertTrue(metaCapture.getValue().getContentLength() > 1);
	}


	@Test
	public void testListBatchKeys() throws Exception {
		
		ObjectListing one = new ObjectListing();
		S3ObjectSummary sum = new S3ObjectSummary();
		sum.setKey("one");
		one.getObjectSummaries().add(sum);
		sum = new S3ObjectSummary();
		sum.setKey("two");
		one.getObjectSummaries().add(sum);
		one.setNextMarker("nextMarker");
		
		ObjectListing two = new ObjectListing();
		sum = new S3ObjectSummary();
		sum.setKey("three");
		two.getObjectSummaries().add(sum);
		sum = new S3ObjectSummary();
		sum.setKey("four");
		two.getObjectSummaries().add(sum);
		two.setMarker(null);
		
		when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(one, two);
		// Now iterate over all key and ensure all keys are found
		Set<String> foundKeys = dao.listAllKeys();
		// the two set should be equal
		assertEquals(4, foundKeys.size());
		assertTrue(foundKeys.contains("one"));
		assertTrue(foundKeys.contains("two"));
		assertTrue(foundKeys.contains("three"));
		assertTrue(foundKeys.contains("four"));
	}

	private List<ExampleObject> buildExampleObjectList(int count) {
		List<ExampleObject> data = new LinkedList<ExampleObject>();
		for(int i=0; i<count; i++){
			ExampleObject ob = new ExampleObject();
			ob.setaBoolean(i%2 == 0);
			ob.setaString("Value,"+i);
			ob.setaLong(new Long(11*i));
			ob.setaDouble(12312312.34234/i);
			ob.setAnInteger(new Integer(i));
			ob.setaFloat(new Float(123.456*i));
			ob.setSomeEnum(SomeEnum.A);
			// Add some nulls
			if(i%3 == 0){
				ob.setaBoolean(null);
			}
			if(i%4 == 0){
				ob.setaString(null);
			}
			if(i%5 == 0){
				ob.setaLong(null);
			}
			data.add(ob);
		}
		return data;
	}

}
