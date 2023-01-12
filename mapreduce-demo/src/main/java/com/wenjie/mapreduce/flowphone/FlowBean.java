package com.wenjie.mapreduce.flowphone;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {


    private Long phone;

    private Long upLine;

    private Long downLine;

    private Long sumLine;


    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(this.getSumLine(), o.getSumLine());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(phone);
        dataOutput.writeLong(upLine);
        dataOutput.writeLong(downLine);
        dataOutput.writeLong(sumLine);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readLong();
        this.upLine = dataInput.readLong();
        this.downLine = dataInput.readLong();
        this.sumLine = dataInput.readLong();
    }

    public Long getPhone() {
        return phone;
    }

    public void setPhone(Long phone) {
        this.phone = phone;
    }

    public Long getUpLine() {
        return upLine;
    }

    public void setUpLine(Long upLine) {
        this.upLine = upLine;
    }

    public Long getDownLine() {
        return downLine;
    }

    public void setDownLine(Long downLine) {
        this.downLine = downLine;
    }

    public Long getSumLine() {
        return sumLine;
    }

    public void setSumLine() {
        this.sumLine = this.upLine + this.downLine;
    }

    @Override
    public String toString() {
        return  upLine + "\t" + downLine + "\t" + sumLine;
    }

    public FlowBean() {
    }

    public FlowBean(Long phone, Long upLine, Long downLine, Long sumLine) {
        this.phone = phone;
        this.upLine = upLine;
        this.downLine = downLine;
        this.sumLine = sumLine;
    }

    public FlowBean(Long upLine, Long downLine, Long sumLine) {
        this.upLine = upLine;
        this.downLine = downLine;
        this.sumLine = sumLine;
    }
}
