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
import eu.proteus.consumer.model.MomentsResult;
import eu.proteus.consumer.model.MomentsResult1D;
import eu.proteus.consumer.model.MomentsResult2D;
import eu.proteus.consumer.model.ProteusData;
import eu.proteus.consumer.model.SensorMeasurement;
import eu.proteus.consumer.model.SensorMeasurement1D;
import eu.proteus.consumer.model.SensorMeasurement2D;

public class ProteusNewSerializer
		implements Closeable, AutoCloseable, Serializer<SensorMeasurement>, Deserializer<Object> {

	/**
	 * Thread-safe kryo instance that handles, serializes and deserializes
	 * PROTEUS POJOS.
	 */

	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(ProteusSerializer.class);

	private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			SensorMeasurementInternalSerializer sensorInternal = new SensorMeasurementInternalSerializer();
			MomentsInternalSerializer momentsInternal = new MomentsInternalSerializer();
			HSMMeasurementInternalSerializer hsmInternal = new HSMMeasurementInternalSerializer();

			kryo.addDefaultSerializer(HSMMeasurement.class, hsmInternal);

			kryo.addDefaultSerializer(SensorMeasurement.class, sensorInternal);
			kryo.addDefaultSerializer(SensorMeasurement1D.class, sensorInternal);
			kryo.addDefaultSerializer(SensorMeasurement2D.class, sensorInternal);

			kryo.addDefaultSerializer(MomentsResult.class, momentsInternal);
			kryo.addDefaultSerializer(MomentsResult1D.class, momentsInternal);
			kryo.addDefaultSerializer(MomentsResult2D.class, momentsInternal);

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
	public byte[] serialize(String topic, SensorMeasurement record) {
		int byteBufferLength = 50;
		ByteBufferOutput output = new ByteBufferOutput(byteBufferLength);
		kryos.get().writeObject(output, record);
		return output.toBytes();
	}

	@Override
	public Object deserialize(String topic, byte[] bytes) {
		if (topic.equals(ProteusData.get("kafka.topicName")) && bytes.length < 40) {
			return kryos.get().readObject(new ByteBufferInput(bytes), SensorMeasurement.class);
		} else if (topic.equals(ProteusData.get("kafka.flatnessTopicName")) && bytes.length < 40) {
			return kryos.get().readObject(new ByteBufferInput(bytes), SensorMeasurement.class);
		} else if (topic.equals(ProteusData.get("kafka.hsmTopicName"))) {
			return kryos.get().readObject(new ByteBufferInput(bytes), HSMMeasurement.class);
		} else if (topic.equals("simple-moments")) {
			return kryos.get().readObject(new ByteBufferInput(bytes), MomentsResult.class);
		} else {
			throw new IllegalArgumentException("Invalid topic name: " + topic);
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
				output.writeDouble(cast.getX());
				output.writeInt(cast.getVarName());
				output.writeDouble(cast.getValue());
			} else {
				SensorMeasurement2D cast = (SensorMeasurement2D) row;
				output.writeInt(MAGIC_NUMBER);
				output.writeByte(row.getType());
				output.writeInt(cast.getCoilID());
				output.writeDouble(cast.getX());
				output.writeDouble(cast.getY());
				output.writeInt(cast.getVarName());
				output.writeDouble(cast.getValue());
			}
		}

		@Override
		public SensorMeasurement read(Kryo kryo, Input input, Class<SensorMeasurement> clazz) {
			int magicNumber = input.readInt();
			assert (magicNumber == MAGIC_NUMBER);

			boolean is2D = (input.readByte() == 0x0001) ? true : false;
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

	private static class MomentsInternalSerializer extends com.esotericsoftware.kryo.Serializer<MomentsResult> {
		@Override
		public void write(Kryo kryo, Output output, MomentsResult row) {
		}

		@Override
		public MomentsResult read(Kryo kryo, Input input, Class<MomentsResult> clazz) {
			int magicNumber = input.readInt();
			assert (magicNumber == MAGIC_NUMBER);

			int coil = input.readInt();
			int var = input.readInt();
			byte type = input.readByte();

			if (type == 0x0) {
				double x = input.readDouble();
				double mean = input.readDouble();
				double variance = input.readDouble();
				double counter = input.readDouble();
				return new MomentsResult1D(coil, var, mean, variance, counter, x);
			} else {
				double x = input.readDouble();
				double y = input.readDouble();
				double mean = input.readDouble();
				double variance = input.readDouble();
				double counter = input.readDouble();
				return new MomentsResult2D(coil, var, mean, variance, counter, x, y);
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
