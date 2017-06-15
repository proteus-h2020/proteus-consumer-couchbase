package eu.proteus.producer.serialization;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Test;

import eu.proteus.consumer.model.SensorMeasurement1D;
import eu.proteus.consumer.model.SensorMeasurement2D;
import eu.proteus.consumer.serialization.ProteusSerializer;

public class ProteusSerializatorTest {

	private ProteusSerializer kryo;

	@Before
	public void initialize() {
		this.kryo = new ProteusSerializer();
	}

	@Test

	public void RealTimeDeserializatino() {
		String test = "{1timestamp1:1494717640919,1coilId1:40103147,1x1:2864.0,1varName1:C0001,1value1:1.846,1type1:1Row2D1,1y1:11.0}";
		SensorMeasurement2D deserialized = (SensorMeasurement2D) this.kryo.deserialize("proteus-realtime",
				test.getBytes());
		assertEquals(test, deserialized);
	}

	@Test
	public void test1DSerializationAndDeserialization() {
		SensorMeasurement1D row = new SensorMeasurement1D(randomInt(), randomDouble(), randomVarIdentifier(),
				randomDouble());
		byte[] bytes = this.kryo.serialize("proteus-realtime", row);

		SensorMeasurement1D deserialized = (SensorMeasurement1D) this.kryo.deserialize("proteus-realtime", bytes);
		assertEquals(row, deserialized);
	}

	@Test
	public void test2DSerializationAndDeserialization() {
		SensorMeasurement2D row = new SensorMeasurement2D(randomInt(), randomDouble(), randomDouble(),
				randomVarIdentifier(), randomDouble());
		byte[] bytes = this.kryo.serialize("proteus-realtime", row);

		SensorMeasurement2D deserialized = (SensorMeasurement2D) this.kryo.deserialize("proteus-realtime", bytes);
		assertEquals(row, deserialized);
	}

	private Map<String, Object> createFakeHSMValues() {
		int size = ThreadLocalRandom.current().nextInt(3000, 8000);
		Map<String, Object> map = new HashMap<String, Object>();

		for (int i = 0; i < size; i++) {
			String varname = "V" + i;
			map.put(varname, new Integer(i));
		}
		return map;
	}

	private double randomDouble() {
		return ThreadLocalRandom.current().nextDouble(0, 30000D);
	}

	private int randomInt() {
		return ThreadLocalRandom.current().nextInt(0, 2000);
	}

	private int randomVarIdentifier() {
		return ThreadLocalRandom.current().nextInt(1, 54);
	}
}
