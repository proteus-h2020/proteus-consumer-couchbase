package eu.proteus.consumer.serialization;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.proteus.consumer.model.HSMMeasurement;
import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.model.ProteusData;
import eu.proteus.consumer.model.SensorMeasurement;
import eu.proteus.consumer.model.SensorMeasurement1D;
import eu.proteus.consumer.model.SensorMeasurement2D;

public class ProteusSerializer implements Closeable, AutoCloseable, Serializer<Measurement>, Deserializer<Measurement> {

	/**
	 * Thread-safe kryo instance that handles, serializes and deserializes
	 * PROTEUS POJOS.
	 */
	private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			SensorMeasurementInternalSerializer sensorInternal = new SensorMeasurementInternalSerializer();
			HSMMeasurementInternalSerializer hsmInternal = new HSMMeasurementInternalSerializer();

			kryo.addDefaultSerializer(HSMMeasurement.class, hsmInternal);
			kryo.addDefaultSerializer(SensorMeasurement.class, sensorInternal);
			kryo.addDefaultSerializer(SensorMeasurement1D.class, sensorInternal);
			kryo.addDefaultSerializer(SensorMeasurement2D.class, sensorInternal);
			return kryo;
		};
	};

	/**
	 * The MAGIC_NUMBER, with the value of the PROTEUS project identificator
	 */
	private static final int MAGIC_NUMBER = 0x00687691; // PROTEUS EU id

	@Override
	public void configure(Map<String, ?> map, boolean b) {
	}

	@Override
	public byte[] serialize(String topic, Measurement record) {
		int byteBufferLength = 50;
		if (record instanceof HSMMeasurement) {
			byteBufferLength = 7600 * 2 * 100; // TODO: improve
		}
		ByteBufferOutput output = new ByteBufferOutput(byteBufferLength);
		kryos.get().writeObject(output, record);
		return output.toBytes();
	}

	@Override
	public Measurement deserialize(String topic, byte[] bytes) {

		if (topic.equals(ProteusData.get("kafka.topicName")) && bytes.length < 40) {
			return kryos.get().readObject(new ByteBufferInput(bytes), SensorMeasurement.class);
		} else if (topic.equals(ProteusData.get("kafka.flatnessTopicName")) && bytes.length < 40) {
			return kryos.get().readObject(new ByteBufferInput(bytes), SensorMeasurement.class);
		} else if (topic.equals(ProteusData.get("kafka.hsmTopicName"))) {
			return kryos.get().readObject(new ByteBufferInput(bytes), HSMMeasurement.class);
		} else {
			throw new IllegalArgumentException("Illegal argument: " + topic);
		}
	}

	@Override
	public void close() {

	}

	private static class SensorMeasurementInternalSerializer
			extends com.esotericsoftware.kryo.Serializer<SensorMeasurement> {
		@Override
		public void write(Kryo kryo, Output output, SensorMeasurement row) {
			if (row instanceof SensorMeasurement1D) {
				SensorMeasurement1D cast = (SensorMeasurement1D) row;
				output.writeInt(MAGIC_NUMBER);
				output.writeByte(row.getType());
				output.writeInt(cast.getCoilID());
				output.writeDouble(cast.getPositionX());
				output.writeInt(cast.getVarName());
				output.writeDouble(cast.getValue());
			} else {
				SensorMeasurement2D cast = (SensorMeasurement2D) row;
				output.writeInt(MAGIC_NUMBER);
				output.writeByte(row.getType());
				output.writeInt(cast.getCoilID());
				output.writeDouble(cast.getPositionX());
				output.writeDouble(cast.getPositionY());
				output.writeInt(cast.getVarName());
				output.writeDouble(cast.getValue());
			}
		}

		@Override
		public SensorMeasurement read(Kryo kryo, Input input, Class<SensorMeasurement> clazz) {
			int magicNumber = input.readInt();
			assert (magicNumber == MAGIC_NUMBER);

			boolean is2D = (input.readByte() == 0x01) ? true : false;
			int coilId = input.readInt();
			double x = input.readDouble();
			double y = (is2D) ? input.readDouble() : 0;
			int varId = input.readInt();
			double value = input.readDouble();

			if (is2D) {
				return new SensorMeasurement2D(coilId, x, y, varId, value);
			} else {
				return new SensorMeasurement1D(coilId, x, varId, value);
			}

		}
	}

	private static class HSMMeasurementInternalSerializer extends com.esotericsoftware.kryo.Serializer<HSMMeasurement> {
		@Override
		public void write(Kryo kryo, Output output, HSMMeasurement hsmRecord) {
			output.writeInt(hsmRecord.getCoilID());
			kryo.writeObject(output, hsmRecord.getHSMVariables());
		}

		@Override
		public HSMMeasurement read(Kryo kryo, Input input, Class<HSMMeasurement> clazz) {
			int coil = input.readInt();
			@SuppressWarnings("unchecked")
			Map<String, Object> variables = kryo.readObject(input, HashMap.class);
			HSMMeasurement hsmRecord = new HSMMeasurement(coil);
			hsmRecord.setVariables(variables);
			return hsmRecord;
		}
	}
}