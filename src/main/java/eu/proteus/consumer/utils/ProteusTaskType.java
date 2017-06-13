package eu.proteus.consumer.utils;

import eu.proteus.consumer.exceptions.InvalidTaskTypeException;
import eu.proteus.consumer.tasks.ProteusFlatnessTask;
import eu.proteus.consumer.tasks.ProteusHSMTask;
import eu.proteus.consumer.tasks.ProteusSimpleMomentsTask;
import eu.proteus.consumer.tasks.ProteusStreamingTask;
import eu.proteus.consumer.tasks.ProteusTask;

public enum ProteusTaskType {

	PROTEUS_FLATNESS(new ProteusFlatnessTask()), PROTEUS_HSM(new ProteusHSMTask()), PROTEUS_REALTIME(
			new ProteusStreamingTask()), PROTEUS_SIMPLE_MOMENTS(new ProteusSimpleMomentsTask());

	private ProteusTask task;

	private ProteusTaskType(ProteusTask task) {
		this.task = task;
	}

	public ProteusTask getWorker() {
		return this.task;
	}

	public static ProteusTask from(String string) throws InvalidTaskTypeException {
		for (ProteusTaskType value : ProteusTaskType.values()) {
			if (value.name().equalsIgnoreCase(string)) {
				return value.task;
			}
		}
		throw new InvalidTaskTypeException("Invalid task type for value " + string);
	}
}
