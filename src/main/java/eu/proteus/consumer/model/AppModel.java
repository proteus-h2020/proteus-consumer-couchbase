package eu.proteus.consumer.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AppModel {

    private Integer aliveCoil;
    private SensorMeasurement lastCoilRow;
    private Date lastCoilStart;
    private ProductionStatus status;

    private List<SensorMeasurement> currentFlatnessRows;

    private double delay;

    public AppModel() {
        this.aliveCoil = null;
        this.status = ProductionStatus.AWAITING;
        this.delay = 0.0D;
        this.lastCoilRow = null;
        this.currentFlatnessRows = new ArrayList<SensorMeasurement>();
    }


    public Integer getAliveCoil() {
        return aliveCoil;
    }

    public void setAliveCoil(Integer aliveCoil) {
        this.aliveCoil = aliveCoil;
    }


    public ProductionStatus getStatus() {
        return status;
    }

    public void setStatus(ProductionStatus status) {
        this.status = status;
    }

    public double getDelay() {
        return delay;
    }

    public void setDelay(double delay) {
        this.delay = delay;
    }

    public SensorMeasurement getLastCoilRow() {
        return lastCoilRow;
    }

    public void setLastCoilRow(SensorMeasurement lastCoilRow) {
        this.lastCoilRow = lastCoilRow;
    }

    public Date getLastCoilStart() {
        return lastCoilStart;
    }

    public void setLastCoilStart(Date lastCoilStart) {
        this.lastCoilStart = lastCoilStart;
    }

    public List<SensorMeasurement> getCurrentFlatnessRows() {
        return currentFlatnessRows;
    }

    public void setCurrentFlatnessRows(List<SensorMeasurement> currentFlatnessRows) {
        this.currentFlatnessRows = currentFlatnessRows;
    }

    public enum ProductionStatus {
        PRODUCING, AWAITING
    }
}
