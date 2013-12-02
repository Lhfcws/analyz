package com.yeezhao.analyz.age.data;

import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSink;
import weka.core.converters.ConverterUtils.DataSource;

/**
 * 样本
 * @author Administrator
 *
 */
public class Sample {
	private Instances instances;

	public Sample() {
		super();
		this.instances = null;
	}
	public Sample(String fileName) {
		super();
		this.instances = getInitInstances(fileName);
	}
	public Sample(Instances instances) {
		super();
		this.instances = instances;
	}
	public void save(String fileName) {
		try {			
			DataSink.write(fileName, instances);
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	public Instances getInitInstances(String fileName) {
		Instances ins = null;
		try {
			ins = DataSource.read(fileName);
			ins.setClassIndex(ins.numAttributes() - 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ins;
	}
	public Instances getInstances() {
		return instances;
	}
	public void setInstances(Instances instances) {
		this.instances = instances;
	}	
}
